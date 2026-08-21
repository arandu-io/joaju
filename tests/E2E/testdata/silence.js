// The pong that does not come.
//
// The Go side gives this server a protocol that swallows pusher:ping, so the
// socket stays open at the transport layer and answers nothing. That is the case
// the ping exists for: a connection that TCP still believes in and that is not
// carrying anything -- a proxy that went away, a laptop that slept, a load
// balancer that kept the socket and lost the backend.
//
// A client that only reconnected on close would sit there forever.

'use strict';

run(async function () {
    var client = connection({ pongTimeout: 500 });

    var established = once(client, 'connected');
    client.connect();
    var first = (await established).data.socketId;

    // The second connection is the whole assertion: the client gave up on a
    // socket nothing had closed, and dialled again.
    var again = once(client, 'connected', 10000);
    var second = (await again).data.socketId;

    // Read before disconnecting: disconnect() closes the socket, and what this
    // is asserting is the state the reconnect left behind.
    var result = {
        first: first,
        second: second,
        changed: first !== second,
        state: client.state
    };
    client.disconnect();

    return result;
});
