// The activity timeout: a client that has heard nothing pings, on the server's
// schedule and not its own.
//
// The Go side gives this server an activity_timeout of one second and records
// the frames it receives, so what this script has to do is stay quiet and stay
// connected. A client that ignored the number would be hung up on by a server
// that means it.

'use strict';

run(async function () {
    var client = connection();
    var established = once(client, 'connected');
    client.connect();
    var socketId = (await established).data.socketId;

    // Long enough for two pings at the timeout the server named, and short
    // enough that a suite does not wait on it.
    await new Promise(function (resolve) {
        setTimeout(resolve, 2500);
    });

    return {
        socketId: socketId,
        // Unchanged: a socket that had been dropped and dialled again would have
        // a new id, and that is the failure this is watching for.
        socketIdAfter: client.socketId,
        state: client.state,
        // What the server told this client to do, in milliseconds. It came off
        // the connection_established frame and not off the options.
        activityTimeout: client._activityTimeout
    };
});
