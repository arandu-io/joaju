// The Joaju browser client: the Pusher protocol, spoken from a page.
//
// A protocol only exists if something on the other end speaks it, and nothing in
// a browser speaks this one. The ready-made browser halves of it arrive through
// npm -- which this project does not have, anywhere (RULE 13). So this file is
// written here and served by the binary, the way HTMX and Alpine already are
// (ADR 0052).
//
// What is deliberately NOT here is the wrapper layer that usually sits on top of
// a client like this one. It resolves a channel name through a service container
// before subscribing, and this project has no container (ADR 0001); what it would
// add on top of this file is a second vocabulary for the same subscribe call.
//
// # It runs as it is written
//
// No build step, no bundler, no transpiler. Classes, template-free strings and
// promises are all ES2017, which every browser this project targets has had for
// years. The file is a classic script that assigns one global, because that is
// how HTMX and Alpine already arrive on the page and a module would mean the
// application writing modules of its own.
//
// # It runs under script-src 'self'
//
// Nothing here compiles a string into code, imports at runtime, assigns markup,
// or names a host. The strictest CSP this project ships is the one this file was
// written against, and the guard for that is a test rather than this paragraph --
// which is also why the constructs it forbids are described here rather than
// spelled, since the test reads this file.
//
// # The shape of the API
//
//     const joaju = new Joaju({ key: 'app-key' })
//     joaju.connect()
//     joaju.on('connected', ({ socketId }) => ...)
//
//     const orders = joaju.subscribe('private-orders.17')
//     orders.bind('OrderShipped', (data) => ...)
//     orders.trigger('client-typing', { who: 'ana' })
//
// Everything a page needs is on those two objects: a connection, and a channel
// per name. The state names -- connecting, connected, unavailable, failed,
// disconnected -- are pusher-js's, so that a developer who has spoken this
// protocol before reads them without being taught (RULE 10).

'use strict';

(function (global) {
    // The protocol's three reserved namespaces, spelled here exactly as joaju.go
    // spells them. An event beginning with one of the first two came from the
    // server; the third is what one browser says to the others.
    var PROTOCOL_PREFIX = 'pusher:';
    var INTERNAL_PREFIX = 'pusher_internal:';
    var CLIENT_PREFIX = 'client-';

    // The two guarded prefixes. The compound kinds -- private-cache- and
    // presence-cache- -- begin with these, so testing the two covers all four,
    // which is what ChannelName.Type does on the server.
    var PRIVATE_PREFIX = 'private-';
    var PRESENCE_PREFIX = 'presence-';

    var EVENT_CONNECTION_ESTABLISHED = PROTOCOL_PREFIX + 'connection_established';
    var EVENT_ERROR = PROTOCOL_PREFIX + 'error';
    var EVENT_SUBSCRIBE = PROTOCOL_PREFIX + 'subscribe';
    var EVENT_UNSUBSCRIBE = PROTOCOL_PREFIX + 'unsubscribe';
    var EVENT_PING = PROTOCOL_PREFIX + 'ping';
    var EVENT_PONG = PROTOCOL_PREFIX + 'pong';

    var EVENT_SUBSCRIPTION_SUCCEEDED = INTERNAL_PREFIX + 'subscription_succeeded';
    var EVENT_MEMBER_ADDED = INTERNAL_PREFIX + 'member_added';
    var EVENT_MEMBER_REMOVED = INTERNAL_PREFIX + 'member_removed';

    // WebSocket.OPEN, named rather than written as 1 at the one place it is
    // compared. The constant is on the constructor, which is not reachable when
    // the environment has no WebSocket at all -- and that case is a clearer error
    // raised in connect().
    var OPEN = 1;

    // FATAL_WINDOW is how long after a refusal a close still counts as being
    // ABOUT that refusal.
    //
    // The protocol puts the reason in a frame and the code ranges say what to do
    // with it -- 4000 to 4099 means do not come back -- but the frame does not
    // say whether the server is about to hang up. It matters, because the server
    // sends 4009 for a subscription it refused and keeps the socket, and 4004 for
    // a connection it refused and drops it immediately afterwards. Both are in
    // the same range.
    //
    // So the client waits to see. A refusal the connection did not survive is a
    // refusal about the connection, and a connection that is still carrying
    // frames a second later was never the thing being refused. The server sends
    // the over-quota frame and terminates in the same breath, so the window only
    // has to cover a queue flush and a close frame.
    var FATAL_WINDOW = 1000;

    // The defaults. Every one of them can be given to the constructor; these are
    // what a page that says nothing gets.
    var DEFAULTS = {
        // authEndpoint is where a private or presence subscription is authorized.
        // It is Pusher's own path, so an application that already has one does
        // not move it.
        authEndpoint: '/broadcasting/auth',
        // authHeaders is what goes on the authorization request on top of the
        // content type -- a CSRF token, usually.
        authHeaders: null,
        // activityTimeout is the fallback for a server that sends no
        // activity_timeout of its own. Two minutes is Pusher's own default.
        activityTimeout: 120000,
        // pongTimeout is how long the answer to pusher:ping may take before the
        // socket is treated as gone. The server hangs up on a socket it has not
        // heard from, so a socket that owes a pong is one the server has already
        // stopped counting on.
        pongTimeout: 30000,
        // The reconnect backoff. It doubles from the minimum to the maximum, and
        // the delay actually waited is drawn uniformly from that range -- see
        // Joaju.prototype._retry for why the randomness is not optional.
        minReconnectDelay: 1000,
        maxReconnectDelay: 30000,
        // url is the socket server, without a path: "wss://socket.example.com".
        // Empty means the page's own origin, which is the mounted deployment.
        url: ''
    };

    // ------------------------------------------------------------------
    // Small shared helpers.
    // ------------------------------------------------------------------

    // unwrap undoes the protocol's string-containing-JSON, and leaves everything
    // else alone.
    //
    // It is the client half of pusher.go's unwrapJSONString and it follows the
    // same rule, including the ambiguity: a string whose contents are not JSON
    // stays the string it is. That ambiguity belongs to the protocol, and a
    // client that resolved it differently from the server would disagree with it
    // about what a payload was.
    function unwrap(data) {
        if (typeof data !== 'string') {
            return data;
        }
        try {
            return JSON.parse(data);
        } catch (err) {
            return data;
        }
    }

    // guarded reports whether a subscription to this name has to be authorized,
    // which is ChannelType.Guarded on the server.
    function guarded(name) {
        return name.indexOf(PRIVATE_PREFIX) === 0 || name.indexOf(PRESENCE_PREFIX) === 0;
    }

    // presence reports whether this channel publishes its subscribers to each
    // other, which is what makes members mean anything.
    function presence(name) {
        return name.indexOf(PRESENCE_PREFIX) === 0;
    }

    // originURL is the page's own origin as a socket address, and is what a
    // mounted deployment connects to.
    function originURL() {
        var location = global.location;
        if (!location || !location.host) {
            return '';
        }

        return (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host;
    }

    // Emitter is the callback table both the connection and a channel keep.
    //
    // A Set per event name rather than an array: binding the same function twice
    // is a mistake that shows up as a message handled twice, and a Set makes
    // unbind able to undo exactly what bind did.
    function Emitter() {
        this._listeners = new Map();
    }

    Emitter.prototype.on = function (event, callback) {
        if (typeof callback !== 'function') {
            throw new TypeError('joaju: a listener is a function');
        }
        var bound = this._listeners.get(event);
        if (!bound) {
            bound = new Set();
            this._listeners.set(event, bound);
        }
        bound.add(callback);

        return this;
    };

    Emitter.prototype.off = function (event, callback) {
        var bound = this._listeners.get(event);
        if (!bound) {
            return this;
        }
        if (callback === undefined) {
            this._listeners.delete(event);
            return this;
        }
        bound.delete(callback);
        if (bound.size === 0) {
            this._listeners.delete(event);
        }

        return this;
    };

    // emit hands one event to every listener bound to it.
    //
    // A listener that throws does not stop the others, and its error is not
    // swallowed either: it is rethrown from a fresh task, where the page's own
    // error reporting sees it. Swallowing would hide the bug; letting it through
    // would let one bad listener cost every other listener its message, and on a
    // socket that means the rest of the page stops updating.
    Emitter.prototype.emit = function (event, first, second) {
        var bound = this._listeners.get(event);
        if (!bound) {
            return;
        }
        bound.forEach(function (callback) {
            try {
                callback(first, second);
            } catch (err) {
                setTimeout(function () {
                    throw err;
                }, 0);
            }
        });
    };

    // ------------------------------------------------------------------
    // Channel
    // ------------------------------------------------------------------

    // Channel is one subscription: what the page binds callbacks to.
    //
    // It is created by Joaju.subscribe and never by hand, because a channel that
    // the connection does not know about is a channel nothing resubscribes after
    // a reconnect.
    function Channel(connection, name) {
        Emitter.call(this);

        this.name = name;
        this.guarded = guarded(name);
        this.presence = presence(name);
        // subscribed is set by the server's confirmation and not by the request:
        // the subscription is a decision a policy makes, and until it says yes
        // nothing is subscribed to anything.
        this.subscribed = false;
        // members is user_id -> user_info, and it is empty off a presence channel.
        // It is kept as a Map rather than rebuilt from the events because a page
        // that renders a member list needs it before the first member_added.
        this.members = new Map();

        this._connection = connection;
    }

    Channel.prototype = Object.create(Emitter.prototype);
    Channel.prototype.constructor = Channel;

    // bind registers a callback for one event on this channel.
    //
    // The event name is the one on the wire, including the pusher_internal:
    // prefix on the three that carry presence. The protocol's own clients rename
    // those to pusher:; this one does not, because a second name for an event is
    // a second thing to keep in step with the server that sends it (RULE 9).
    //
    // The callback receives the payload, already decoded, and a second argument
    // carrying { event, channel, userId } -- userId being the sender of a client
    // event, as the channel recorded them when it seated them, and undefined on
    // everything else.
    Channel.prototype.bind = function (event, callback) {
        return this.on(event, callback);
    };

    Channel.prototype.unbind = function (event, callback) {
        return this.off(event, callback);
    };

    // trigger publishes a client event: one browser talking to the others on this
    // channel.
    //
    // The three refusals here are the client-side halves of the server's, and
    // they are made here so that a mistake is a thrown error at the call site
    // rather than a 4009 arriving later with nothing to attribute it to. The
    // server refuses all three again -- ClientEvents.Accept is where the decision
    // is actually made, and it also refuses when the deployment relays no client
    // events at all, which the browser has no way to know.
    Channel.prototype.trigger = function (event, data) {
        if (typeof event !== 'string' || event.indexOf(CLIENT_PREFIX) !== 0) {
            throw new Error('joaju: a client event is named "' + CLIENT_PREFIX + '..."');
        }
        if (!this.guarded) {
            throw new Error('joaju: a client event goes out on a private or presence channel only, not on ' + this.name);
        }
        if (!this.subscribed) {
            throw new Error('joaju: this connection is not subscribed to ' + this.name);
        }

        return this._connection._send({
            event: event,
            channel: this.name,
            data: data === undefined ? {} : data
        });
    };

    // _succeeded records the confirmation, and on a presence channel the member
    // list that came with it.
    //
    // The list arrives as Reverb's presence block -- { presence: { count, ids,
    // hash } } -- and hash is user_id -> user_info, which is the whole of what a
    // member is here.
    Channel.prototype._succeeded = function (data) {
        this.subscribed = true;
        this.members.clear();
        if (!this.presence || !data || typeof data !== 'object') {
            return;
        }

        var block = data.presence;
        if (!block || typeof block !== 'object') {
            return;
        }
        var hash = block.hash && typeof block.hash === 'object' ? block.hash : {};
        var ids = Array.isArray(block.ids) ? block.ids : Object.keys(hash);
        for (var i = 0; i < ids.length; i++) {
            var id = String(ids[i]);
            this.members.set(id, Object.prototype.hasOwnProperty.call(hash, id) ? hash[id] : null);
        }
    };

    Channel.prototype._memberAdded = function (member) {
        if (!member || member.user_id === undefined || member.user_id === null) {
            return;
        }
        this.members.set(String(member.user_id), member.user_info === undefined ? null : member.user_info);
    };

    Channel.prototype._memberRemoved = function (member) {
        if (!member || member.user_id === undefined || member.user_id === null) {
            return;
        }
        this.members.delete(String(member.user_id));
    };

    // _reset forgets everything the server told us, and is what a lost socket
    // does to a channel.
    //
    // The callbacks stay bound: the page asked to hear about this channel, and a
    // reconnect is not the page changing its mind. What goes is the subscription
    // and the member list, because both were facts about a socket that no longer
    // exists -- a member list carried across a reconnect is a list of who was
    // there, presented as who is there.
    Channel.prototype._reset = function () {
        this.subscribed = false;
        this.members.clear();
    };

    // ------------------------------------------------------------------
    // Joaju
    // ------------------------------------------------------------------

    // Joaju is one connection to one server.
    //
    // options are merged over DEFAULTS; key is the only one required, and it is
    // the {appKey} of the socket route.
    function Joaju(options) {
        Emitter.call(this);

        var settings = options || {};
        if (!settings.key) {
            throw new Error('joaju: a connection needs the app key, which is the {appKey} of the socket route');
        }

        this.options = {};
        for (var name in DEFAULTS) {
            if (Object.prototype.hasOwnProperty.call(DEFAULTS, name)) {
                this.options[name] = settings[name] === undefined ? DEFAULTS[name] : settings[name];
            }
        }
        this.options.key = settings.key;

        // state is one of: initialized, connecting, connected, unavailable,
        // disconnected, failed. They are pusher-js's names (RULE 10).
        this.state = 'initialized';
        // socketId is what the server minted for this socket, and what a publish
        // quotes so that the publisher does not receive its own message. It is
        // null whenever there is no established socket, which is also what says
        // that a subscription cannot be sent yet.
        this.socketId = null;

        this._channels = new Map();
        this._socket = null;
        // _wanted is whether the page asked to be connected. It is what separates
        // a socket that dropped -- reconnect -- from one the page closed.
        this._wanted = false;
        this._attempts = 0;
        this._retryTimer = null;
        this._activityTimer = null;
        this._pongTimer = null;
        this._activityTimeout = this.options.activityTimeout;
        // _refusedCode and _refusedAt are the last refusal in the do-not-return
        // range and when it arrived. See FATAL_WINDOW.
        this._refusedCode = 0;
        this._refusedAt = 0;
        // _retryNow is set by a refusal in the 4200 range, which means come back
        // immediately rather than after the backoff.
        this._retryNow = false;
    }

    Joaju.prototype = Object.create(Emitter.prototype);
    Joaju.prototype.constructor = Joaju;

    // connect opens the socket, and does nothing if one is already open or
    // opening.
    Joaju.prototype.connect = function () {
        this._wanted = true;
        if (this._socket || this._retryTimer) {
            return this;
        }
        this._open();

        return this;
    };

    // disconnect closes the socket and stops reconnecting.
    //
    // The channels are kept, so a later connect() subscribes to them again. That
    // is the same path a reconnect takes, and it is why there is only one of them.
    Joaju.prototype.disconnect = function () {
        this._wanted = false;
        this._clearRetry();

        var socket = this._socket;
        if (!socket) {
            this._setState('disconnected');
            return this;
        }
        // The close frame is written by the browser's own stack, and onclose runs
        // afterwards -- where _wanted being false is what stops the reconnect.
        socket.close(1000, 'client');

        return this;
    };

    // subscribe asks to listen on a channel, and answers the Channel to bind to.
    //
    // Calling it twice for one name answers the same Channel rather than a second
    // one: the server seats one subscription per socket per channel, so two
    // objects would be two views of one seat, and unsubscribing through one of
    // them would silently stop the other.
    //
    // The request goes out now if the socket is established, and at the next
    // connection_established otherwise. Either way the page can bind immediately.
    Joaju.prototype.subscribe = function (name) {
        if (typeof name !== 'string' || name === '') {
            throw new Error('joaju: a subscription needs a channel name');
        }

        var channel = this._channels.get(name);
        if (!channel) {
            channel = new Channel(this, name);
            this._channels.set(name, channel);
        }
        if (this.socketId) {
            this._join(channel);
        }

        return channel;
    };

    // unsubscribe stops listening, and forgets the channel so that a reconnect
    // does not bring it back.
    //
    // Nothing is expected in reply. The protocol confirms a subscription and not
    // a departure, and the server treats an unsubscribe from a channel nobody is
    // on as the reconnect racing its own cleanup that it usually is.
    Joaju.prototype.unsubscribe = function (name) {
        var channel = this._channels.get(name);
        if (!channel) {
            return this;
        }
        this._channels.delete(name);
        channel._reset();

        if (this.socketId) {
            this._send({ event: EVENT_UNSUBSCRIBE, data: { channel: name } });
        }

        return this;
    };

    // channel is the Channel for a name this connection is holding, or undefined.
    Joaju.prototype.channel = function (name) {
        return this._channels.get(name);
    };

    // channels is every channel this connection is holding.
    Joaju.prototype.channels = function () {
        var all = [];
        this._channels.forEach(function (channel) {
            all.push(channel);
        });

        return all;
    };

    // _url is the socket address: the server, then the route, then the app key.
    Joaju.prototype._url = function () {
        var base = this.options.url || originURL();
        if (!base) {
            throw new Error('joaju: there is no page origin to derive the socket address from -- pass url');
        }

        return base.replace(/\/+$/, '') + '/app/' + encodeURIComponent(this.options.key);
    };

    // _open dials.
    //
    // Nothing is done in onopen: the socket being open is the transport's news,
    // and the connection is not established until the server has said so and
    // handed over a socket id. A page told it was connected before that has no id
    // to publish with.
    Joaju.prototype._open = function () {
        var Socket = global.WebSocket;
        if (typeof Socket !== 'function') {
            throw new Error('joaju: this environment has no WebSocket');
        }

        this._setState('connecting');

        var connection = this;
        var socket = new Socket(this._url());
        this._socket = socket;

        socket.onmessage = function (event) {
            // A late frame from a socket already replaced is not this
            // connection's business, and acting on one would let a dead socket
            // resubscribe the channels of the live one.
            if (connection._socket !== socket) {
                return;
            }
            connection._receive(event.data);
        };
        socket.onclose = function (event) {
            if (connection._socket !== socket) {
                return;
            }
            connection._closed(event);
        };
        socket.onerror = function () {
            // Deliberately empty. The error event carries nothing a page can act
            // on -- the specification says so, to keep a page from probing the
            // network -- and onclose always follows it, which is where the
            // reconnect is decided.
        };
    };

    // _receive reads one frame.
    Joaju.prototype._receive = function (raw) {
        // Before anything is parsed: a frame arriving is the proof of life the
        // activity timer exists to wait for, whatever the frame turns out to say.
        this._touch();

        var frame;
        try {
            frame = JSON.parse(raw);
        } catch (err) {
            return;
        }
        if (!frame || typeof frame.event !== 'string') {
            return;
        }

        var data = unwrap(frame.data);

        if (frame.event === EVENT_CONNECTION_ESTABLISHED) {
            this._established(data);
            return;
        }
        if (frame.event === EVENT_ERROR) {
            this._refused(data);
            return;
        }
        if (frame.event === EVENT_PING) {
            // The server has its own WebSocket ping and does not normally send
            // this one, but the protocol has it in both directions and a client
            // that ignored it would look dead to a server that did.
            this._send({ event: EVENT_PONG });
            return;
        }
        if (frame.event === EVENT_PONG) {
            // Nothing is owed to a pong: _touch above already recorded everything
            // it proves.
            return;
        }

        if (typeof frame.channel !== 'string' || frame.channel === '') {
            return;
        }
        var channel = this._channels.get(frame.channel);
        if (!channel) {
            // A channel this connection unsubscribed from while the frame was in
            // flight. Delivering it would call back into a page that already said
            // it had stopped listening.
            return;
        }

        if (frame.event === EVENT_SUBSCRIPTION_SUCCEEDED) {
            channel._succeeded(data);
        } else if (frame.event === EVENT_MEMBER_ADDED) {
            channel._memberAdded(data);
        } else if (frame.event === EVENT_MEMBER_REMOVED) {
            channel._memberRemoved(data);
        }

        channel.emit(frame.event, data, {
            event: frame.event,
            channel: frame.channel,
            userId: frame.user_id
        });
    };

    // _established is pusher:connection_established, which is the first frame on
    // the socket and the one that makes it usable.
    Joaju.prototype._established = function (data) {
        var payload = data && typeof data === 'object' ? data : {};

        this.socketId = payload.socket_id === undefined ? null : String(payload.socket_id);

        // The server's number, in seconds, and it wins over the option: it is the
        // server saying how long it will tolerate silence, and a client pinging
        // on its own schedule is a client that either wastes frames or gets hung
        // up on.
        var seconds = Number(payload.activity_timeout);
        this._activityTimeout = isFinite(seconds) && seconds > 0
            ? seconds * 1000
            : this.options.activityTimeout;

        this._attempts = 0;
        this._refusedCode = 0;
        this._refusedAt = 0;
        this._retryNow = false;

        this._setState('connected');
        this.emit('connected', { socketId: this.socketId });

        // The channels this connection had, on the socket it now has. Every one
        // of them is a fresh decision by the server's policy, and a guarded one
        // is authorized again from scratch -- the signature covers the socket id,
        // and the socket id is exactly what just changed.
        var connection = this;
        this._channels.forEach(function (channel) {
            channel._reset();
            connection._join(channel);
        });

        // Again, with the timeout the server just named rather than the one the
        // timer at the top of _receive was started with.
        this._touch();
    };

    // _refused is pusher:error.
    //
    // The frame carries a code and a sentence and NOT a channel -- the server's
    // ErrorFrame has no channel field, deliberately, because the sentence a policy
    // wrote names the subject and belongs in the server's log rather than in the
    // browser. So a refusal cannot be attributed to the subscription that caused
    // it, and this client does not guess: it reports the code to whoever is
    // listening on the connection and leaves the channels alone.
    //
    // What it does decide is whether to come back, and the ranges are the
    // protocol's:
    //
    //     4000-4099  do not come back      4004 over quota, 4009 unauthorized
    //     4100-4199  come back, backed off
    //     4200-4299  come back now
    //     4300-4399  the frame was refused, the socket was not   4301
    //
    // The last range is the one that changes nothing here, and it is the common
    // one: 4301 is a frame past the rate limit, or a client event on a server
    // that relays none. The socket stays up and the page goes on using it.
    Joaju.prototype._refused = function (data) {
        var payload = data && typeof data === 'object' ? data : {};
        var code = Number(payload.code) || 0;
        var message = typeof payload.message === 'string' ? payload.message : '';

        if (code >= 4000 && code <= 4099) {
            this._refusedCode = code;
            this._refusedAt = Date.now();
        }
        if (code >= 4200 && code <= 4299) {
            this._retryNow = true;
        }

        this.emit('error', { code: code, message: message });
    };

    // _closed is the socket ending, for any reason.
    Joaju.prototype._closed = function (event) {
        this._clearTimers();
        this._socket = null;
        this.socketId = null;

        this._channels.forEach(function (channel) {
            channel._reset();
        });

        var code = event && typeof event.code === 'number' ? event.code : 0;
        this._setState('disconnected');
        this.emit('disconnected', { code: code });

        if (this._fatal(code)) {
            // Nothing about coming back would be different, so coming back is a
            // loop. The page is told, and it is the page's business whether to
            // sign in again and call connect().
            this._setState('failed');
            this.emit('failed', { code: this._refusedCode || code });
            return;
        }
        if (!this._wanted) {
            return;
        }

        this._retry();
    };

    // _fatal reports whether this close is one to come back from.
    //
    // Two things can say no. A close code in the do-not-return range is the
    // server saying it outright, which is what a Pusher-compatible server that
    // closes with the protocol's code does. A refusal in that range moments
    // earlier is this server, which sends the frame and then closes with 1000 --
    // see FATAL_WINDOW for why the moments are what makes it answerable.
    Joaju.prototype._fatal = function (code) {
        if (code >= 4000 && code <= 4099) {
            return true;
        }

        return this._refusedCode !== 0 && (Date.now() - this._refusedAt) <= FATAL_WINDOW;
    };

    // _retry schedules the next attempt.
    //
    // The delay doubles, and it is then drawn at random from between the minimum
    // and that. The randomness is the point rather than a refinement: without it
    // every browser that was connected to a server restarts its backoff at the
    // same instant and comes back at the same instant, so the server's first
    // moment of life is its heaviest -- and the reconnect storm that knocks it
    // over restarts the whole cycle, synchronized a little more tightly each time.
    Joaju.prototype._retry = function () {
        var options = this.options;
        var minimum = options.minReconnectDelay;
        var delay = minimum;

        if (this._retryNow) {
            this._retryNow = false;
        } else {
            var capped = Math.min(options.maxReconnectDelay, minimum * Math.pow(2, this._attempts));
            delay = minimum + Math.random() * Math.max(0, capped - minimum);
        }
        this._attempts++;

        this._setState('unavailable');

        var connection = this;
        this._retryTimer = setTimeout(function () {
            connection._retryTimer = null;
            if (!connection._wanted) {
                return;
            }
            connection._open();
        }, delay);
    };

    // _join sends the subscription, fetching the authorization first when the
    // channel needs one.
    Joaju.prototype._join = function (channel) {
        var socketId = this.socketId;
        if (!socketId) {
            return;
        }
        if (!channel.guarded) {
            this._send({ event: EVENT_SUBSCRIBE, data: { channel: channel.name } });
            return;
        }

        var connection = this;
        this._authorize(channel.name, socketId).then(function (authorization) {
            // The socket may have been replaced while the request was in flight,
            // and a signature is computed over the socket id -- so a late answer
            // authorizes a socket that no longer exists. The reconnect asks again.
            if (connection.socketId !== socketId) {
                return;
            }
            if (connection._channels.get(channel.name) !== channel) {
                return;
            }

            var data = { channel: channel.name, auth: authorization.auth };
            if (authorization.channel_data !== undefined && authorization.channel_data !== null) {
                data.channel_data = authorization.channel_data;
            }
            connection._send({ event: EVENT_SUBSCRIBE, data: data });
        }).catch(function (err) {
            // A code of zero says this refusal is the client's own and not the
            // server's: nothing was sent, so no code came back.
            connection.emit('error', {
                code: 0,
                message: 'joaju: authorizing ' + channel.name + ' failed: ' + err.message,
                channel: channel.name
            });
        });
    };

    // _authorize asks the application whether this socket may have this channel.
    //
    // It is Pusher's shape: a form-encoded POST carrying socket_id and
    // channel_name, answered with { auth } and, for a presence channel,
    // { channel_data }. Form-encoded rather than JSON because that is what the
    // endpoint on the other end already reads.
    //
    // The credentials go because that is the whole mechanism: the endpoint is the
    // application's own, behind its own session, and what it answers is a decision
    // about the signed-in user. Nothing here reads the answer -- the auth string
    // is carried to the server verbatim, where a policy weighs it as evidence.
    Joaju.prototype._authorize = function (channel, socketId) {
        var fetcher = global.fetch;
        if (typeof fetcher !== 'function') {
            return Promise.reject(new Error('this environment has no fetch'));
        }

        var headers = { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' };
        var extra = this.options.authHeaders;
        if (extra) {
            for (var name in extra) {
                if (Object.prototype.hasOwnProperty.call(extra, name)) {
                    headers[name] = extra[name];
                }
            }
        }

        var body = new URLSearchParams();
        body.set('socket_id', socketId);
        body.set('channel_name', channel);

        return fetcher(this.options.authEndpoint, {
            method: 'POST',
            credentials: 'same-origin',
            headers: headers,
            body: body.toString()
        }).then(function (response) {
            if (!response.ok) {
                throw new Error('the endpoint answered ' + response.status);
            }
            return response.json();
        }).then(function (payload) {
            if (!payload || typeof payload.auth !== 'string') {
                throw new Error('the endpoint answered no auth');
            }
            return payload;
        });
    };

    // _send writes one frame, and answers whether it went.
    //
    // A frame sent on a socket that is not open is dropped rather than queued.
    // What would be queued is a subscription, and a subscription is rebuilt from
    // _channels at the next connection_established anyway -- a queue would send it
    // twice. A client event dropped this way is a message the page can send again;
    // one delivered a minute late, after a reconnect, is a message about a moment
    // that has passed.
    Joaju.prototype._send = function (frame) {
        var socket = this._socket;
        if (!socket || socket.readyState !== OPEN) {
            return false;
        }
        socket.send(JSON.stringify(frame));

        return true;
    };

    // _touch restarts the silence clock, and is called for every frame that
    // arrives.
    //
    // The clock measures the SERVER's silence, which is what activity_timeout is
    // about, so nothing this client sends resets it. A pong outstanding is
    // cancelled here too: any frame at all is the proof the ping was asking for.
    Joaju.prototype._touch = function () {
        if (this._pongTimer !== null) {
            clearTimeout(this._pongTimer);
            this._pongTimer = null;
        }
        if (this._activityTimer !== null) {
            clearTimeout(this._activityTimer);
        }

        var connection = this;
        this._activityTimer = setTimeout(function () {
            connection._activityTimer = null;
            connection._ping();
        }, this._activityTimeout);
    };

    // _ping asks whether the server is still there, and gives it pongTimeout to
    // say so.
    //
    // The protocol has a ping of its own because a page cannot send a WebSocket
    // control frame from JavaScript: the transport's keepalive is not reachable
    // from here, so the protocol carries a second one that is.
    Joaju.prototype._ping = function () {
        if (!this._send({ event: EVENT_PING })) {
            return;
        }

        var connection = this;
        this._pongTimer = setTimeout(function () {
            connection._pongTimer = null;
            // Silence answered with silence. The socket is gone whatever the
            // browser still believes about it, and closing it here is what turns
            // a socket nobody is listening on into a reconnect: onclose is the one
            // path that schedules one.
            var socket = connection._socket;
            if (socket) {
                socket.close(1000, 'no pong');
            }
        }, this.options.pongTimeout);
    };

    Joaju.prototype._clearTimers = function () {
        if (this._activityTimer !== null) {
            clearTimeout(this._activityTimer);
            this._activityTimer = null;
        }
        if (this._pongTimer !== null) {
            clearTimeout(this._pongTimer);
            this._pongTimer = null;
        }
    };

    Joaju.prototype._clearRetry = function () {
        if (this._retryTimer !== null) {
            clearTimeout(this._retryTimer);
            this._retryTimer = null;
        }
    };

    // _setState records the state and announces a change, once.
    Joaju.prototype._setState = function (state) {
        if (this.state === state) {
            return;
        }
        var previous = this.state;
        this.state = state;
        this.emit('state', { previous: previous, current: state });
    };

    Joaju.Channel = Channel;

    global.Joaju = Joaju;
}(typeof globalThis !== 'undefined' ? globalThis : this));
