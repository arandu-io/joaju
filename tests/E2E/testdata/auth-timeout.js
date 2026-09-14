'use strict';

run(async function () {
    var client = connection({
        authEndpoint: HTTP + '/broadcasting/hang',
        authTimeout: 75
    });
    var connected = once(client, 'connected');
    client.connect();
    await connected;

    var started = Date.now();
    var failed = once(client, 'error', 2000);
    client.subscribe('private-never-answered');
    var result = await failed;
    client.disconnect();

    return {
        code: result.data.code,
        message: result.data.message,
        elapsed: Date.now() - started
    };
});
