// The reconnect, and the resubscription that has to come with it.
//
// The socket is closed by the server, through one of its own nine routes, which
// is what a deploy looks like from a browser. What has to happen next is not just
// a new socket: the channels have to come back, and the private one has to be
// authorized again from scratch -- the signature covers the socket id, and the
// socket id is exactly what changed.
//
// The proof is an event arriving on the new socket, on a channel nobody
// subscribed to twice from here.

'use strict';

run(async function () {
    var client = connection();
    var established = once(client, 'connected');
    client.connect();
    var first = (await established).data.socketId;

    var orders = await subscribed(client, 'orders');
    var invoices = await subscribed(client, 'private-invoices.17');

    // Everything that has to happen after the drop is awaited from here, before
    // the drop, so that nothing is bound in a gap the server could answer in.
    //
    // A page binds its callbacks once and expects them to survive a reconnect,
    // which is the other half of what this is checking: a client that reset them
    // would be a client whose updates stopped silently the first time a server
    // was replaced.
    var again = once(client, 'connected', 10000);
    var publicAgain = once(orders, 'pusher_internal:subscription_succeeded', 15000);
    var privateAgain = once(invoices, 'pusher_internal:subscription_succeeded', 15000);
    var delivered = once(orders, 'OrderShipped', 15000);
    var privately = once(invoices, 'InvoicePaid', 15000);

    await terminate('ana');

    var second = (await again).data.socketId;

    // The subscriptions are not confirmed at the instant the connection is: each
    // one is a fresh decision, and the private one waits on the endpoint first.
    await publicAgain;
    await privateAgain;

    await publish('orders', 'OrderShipped', { id: 9 });
    await publish('private-invoices.17', 'InvoicePaid', { id: 11 });

    var shipped = await delivered;
    var paid = await privately;

    // Read before disconnecting: disconnect() closes the socket, and what this is
    // asserting is the state the reconnect left behind.
    var result = {
        first: first,
        second: second,
        changed: first !== second,
        publicEvent: shipped.data,
        privateEvent: paid.data,
        publicSubscribed: orders.subscribed,
        privateSubscribed: invoices.subscribed,
        // Two channels held, and not four: resubscribing must not have made
        // second copies of the ones that were already there.
        held: client.channels().length
    };
    client.disconnect();

    return result;
});
