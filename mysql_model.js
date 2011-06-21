var mysql = require('db-mysql');
var utils = require('./utils');
var Deferred = require('./deferred');

/**
* Create a mysql-backed model for the given `model`, where `options` describes the table structure for model objects.
*
* @param {Function} model - Constructor for a model class
* @param {Object} options - Object describing table structure for given class
**/
var MySqlModel = module.exports = function(model, pool, options) {
    if ( 'function' !== typeof(model) ) {
        throw new Error('Model must be a constructor');
    }

    if ( arguments.length === 2 ) {
        // pool was given as an option
        options = pool;
        pool = options.pool;
    }

    if ( arguments.length === 1 ) {
        // pool and model were given as options
        options = model;
        model = options.model;
        pool = options.pool;
    }

    if ( !model ) {
        throw new Error('No model was specified');
    }

    if ( !pool ) {
        throw new Error('No pool was specified');
    }

    var allFields = Object.keys(options.fields).map(function(key) { return options.fields[key].field; });
    var tableName = options.table;
    var idField = Object.keys(options.fields).map(function(key) { return options.fields[key]; })
                        .filter(function(fieldInfo) { return fieldInfo.id; } )
                        .map(function(fieldInfo) { return fieldInfo.field; })[0]; // TODO: support compound ids

    model.get_by_id = function(id) {
        var deferred = new Deferred();

        if ( !~['number', 'string'].indexOf(typeof(id)) ) {
            deferred.reject(Error('id must be a number or string.  It is a ' + typeof(id)));
        }

        pool.acquire(function(err, db) {
            if ( err ) {
                throw err;
            }

            db.query().select(allFields).from(tableName).where(idField + '=?', [id]).execute(function(error, rows, cols) {
                pool.release(db);

                console.log(rows);
                if ( error ) {
                    throw error;
                }

                if ( rows.length > 1 ) {
                    throw new Error('get_by_id should never return multiple records');
                }

                if ( rows.length < 1 ) {
                    return null;
                }

                deferred.resolve(utils.classify(rows[0], model));
            });
        });

        return deferred.promise();
    }
    
    model.get_by_id.ajaxify = true;

    return model;
}
