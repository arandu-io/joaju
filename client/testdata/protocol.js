// The protocol, end to end, through two browsers.
//
// One pass: connect, subscribe to each kind of channel, receive an event
// published over the HTTP API, watch a second client join a presence channel,
// whisper to it, be refused a channel, and watch it leave.

'use strict';

run(async function () {
    var result = {};

    // Connecting is not the socket opening: it is the server having sent
    // pusher:connection_established with a socket id in it.
    var ana = connection({ authHeaders: { 'X-Joaju-User': 'ana' } });
    var established = once(ana, 'connected');
    ana.connect();
    result.socketId = (await established).data.socketId;
    result.state = ana.state;

    // A public channel needs no authorization, so nothing is fetched.
    var orders = await subscribed(ana, 'orders');
    result.publicSubscribed = orders.subscribed;

    // A private one goes through the application's endpoint first, and the
    // signature it answers with is carried in the subscribe frame.
    var invoices = await subscribed(ana, 'private-invoices.17');
    result.privateSubscribed = invoices.subscribed;

    // A presence one carries channel_data as well, and comes back with the
    // member list -- this subscriber included.
    var room = await subscribed(ana, 'presence-room.1');
    result.membersAtFirst = members(room);

    // An event published over the HTTP API reaches the subscriber.
    var delivered = once(orders, 'OrderShipped');
    await publish('orders', 'OrderShipped', { id: 7 });
    var shipped = await delivered;
    result.event = shipped.data;
    result.eventChannel = shipped.meta.channel;

    // A second browser, as a different person, joins the presence channel. The
    // first one is told.
    var joined = once(room, 'pusher_internal:member_added');
    var bruno = connection({ authHeaders: { 'X-Joaju-User': 'bruno' } });
    var brunoConnected = once(bruno, 'connected');
    bruno.connect();
    await brunoConnected;
    var brunoRoom = await subscribed(bruno, 'presence-room.1');

    result.memberAdded = (await joined).data;
    result.membersAfterJoin = members(room);
    result.brunoMembers = members(brunoRoom);

    // A client event: one browser talking to the other, with the server only
    // relaying it. The user_id on the frame is the one the channel seated, and
    // never the one the sender wrote.
    var whispered = once(room, 'client-typing');
    brunoRoom.trigger('client-typing', { at: 'the keyboard' });
    var typing = await whispered;
    result.clientEvent = typing.data;
    result.clientEventUser = typing.meta.userId;

    // A channel the policy refuses is 4009, and the socket lives through it: the
    // frame was refused, not the connection.
    var refused = once(ana, 'error');
    ana.subscribe('private-forbidden');
    result.refusedCode = (await refused).data.code;
    result.stillConnected = ana.state === 'connected';
    result.stillSubscribed = orders.subscribed && invoices.subscribed && room.subscribed;

    // Leaving is announced to whoever is left, and the member list shrinks.
    var left = once(room, 'pusher_internal:member_removed');
    bruno.unsubscribe('presence-room.1');
    result.memberRemoved = (await left).data;
    result.membersAfterLeave = members(room);

    // A client event on a public channel is refused here rather than by the
    // server, because there is nothing to be gained by asking.
    try {
        orders.trigger('client-typing', {});
        result.publicWhisperRefused = false;
    } catch (err) {
        result.publicWhisperRefused = true;
    }

    ana.disconnect();
    bruno.disconnect();

    return result;
});
