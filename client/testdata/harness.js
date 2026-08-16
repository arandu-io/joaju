// The shared half of every scenario: how to reach the server the Go test
// started, and how to report what happened.
//
// This file is concatenated after joaju.js and before one scenario, so there is
// no module system in play and everything below is simply in scope. Nothing here
// is part of the product.

'use strict';

var HTTP = process.env.JOAJU_HTTP;
var WS = process.env.JOAJU_WS;
var KEY = process.env.JOAJU_KEY;
var APP = process.env.JOAJU_APP;

// connection is one client, pointed at the test server.
//
// The backoff is shortened for every scenario, not only the ones that reconnect:
// a scenario that fails should fail in the second it takes to notice rather than
// after the production minimum.
function connection(options) {
    var settings = {
        key: KEY,
        url: WS,
        authEndpoint: HTTP + '/broadcasting/auth',
        minReconnectDelay: 50,
        maxReconnectDelay: 200
    };
    for (var name in options || {}) {
        if (Object.prototype.hasOwnProperty.call(options, name)) {
            settings[name] = options[name];
        }
    }

    return new globalThis.Joaju(settings);
}

// once resolves with the first occurrence of one event, and rejects if it does
// not arrive.
//
// A rejection rather than a hang, because a scenario that stops waiting says
// which event never came -- and a hang says only that the runtime was killed
// sixty seconds later.
function once(emitter, event, milliseconds) {
    return new Promise(function (resolve, reject) {
        var timer = setTimeout(function () {
            emitter.off(event, handler);
            reject(new Error('waiting for ' + event + ' timed out'));
        }, milliseconds || 5000);

        function handler(data, meta) {
            clearTimeout(timer);
            emitter.off(event, handler);
            resolve({ data: data, meta: meta });
        }

        emitter.on(event, handler);
    });
}

// subscribed subscribes and waits for the server to confirm it.
function subscribed(client, name) {
    var channel = client.subscribe(name);

    return once(channel, 'pusher_internal:subscription_succeeded').then(function () {
        return channel;
    });
}

// publish sends an event over the server's own HTTP API, which is how an
// application publishes.
//
// It is one of the nine routes, so a scenario needs nothing from the Go side to
// make an event happen: the whole exchange is driven from here.
function publish(channel, name, data) {
    return fetch(HTTP + '/apps/' + APP + '/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            name: name,
            channel: channel,
            // The protocol carries a payload as a JSON string containing JSON,
            // in both directions.
            data: JSON.stringify(data)
        })
    }).then(function (response) {
        if (!response.ok) {
            throw new Error('publishing answered ' + response.status);
        }
    });
}

// terminate closes every socket the server holds for one user, which is how a
// scenario makes a connection drop without touching the socket from here.
function terminate(user) {
    return fetch(HTTP + '/apps/' + APP + '/users/' + user + '/terminate_connections', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}'
    }).then(function (response) {
        if (!response.ok) {
            throw new Error('terminating answered ' + response.status);
        }
    });
}

// members is a channel's member list as a sorted array of ids, which is what an
// assertion compares.
function members(channel) {
    return Array.from(channel.members.keys()).sort();
}

// run drives one scenario and prints its result as the process's only output.
//
// Standard output is the result and standard error is the diagnosis, so the Go
// side can parse the first without stripping the second.
function run(main) {
    var guard = setTimeout(function () {
        process.stderr.write('the scenario did not finish\n');
        process.exit(2);
    }, 20000);

    main().then(function (result) {
        clearTimeout(guard);
        process.stdout.write(JSON.stringify(result));
        process.exit(0);
    }).catch(function (err) {
        clearTimeout(guard);
        process.stderr.write((err && err.stack ? err.stack : String(err)) + '\n');
        process.exit(1);
    });
}
