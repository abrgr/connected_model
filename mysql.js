var mysql = require('db-mysql'),
    generic_pool = require('generic-pool');

var pool = generic_pool.Pool({
    name: 'mysql',
    max: 10,
    create: function(callback) {
        new mysql.Database({
            hostname: 'localhost',
            user: 'root',
            password: 'djudeman',
            database: 'test'}).connect(function(err, server) {
                callback(err, this);
            });
    },
    destroy: function(db) {
        db.disconnect();
    }
});

pool.acquire(function(err, db) {
    if ( err ) {
        console.log('Error: ' + err);
    }

    db.query().select('* from try_this').execute(function(error, rows, cols) {
        pool.release(db);

        if ( error ) {
            console.log('ERROR: ' + error);
        }

        console.log(rows[0]);
    });
});
