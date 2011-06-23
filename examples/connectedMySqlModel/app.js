
/**
 * Module dependencies.
 */

var express = require('express');
var User = require('./models/user');
var connected_model = require('../../index');
var generic_pool = require('generic-pool');
var mysql = require('db-mysql');

var MySqlModel = connected_model.MySqlModel;

var app = module.exports = express.createServer();



app.configure('development', function(){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});

app.configure('production', function(){
  app.use(express.errorHandler()); 
});

// Models
var pool = generic_pool.Pool({
    name: 'mysql',
    max: 10,
    create: function(callback) {
        new mysql.Database({
            hostname: 'localhost',
            user: 'root',
            password: 'dudeman',
            database: 'test'}).connect(function(err, server) {
                callback(err, this);
            });
    },
    destroy: function(db) {
        db.disconnect();
    }
});

User = new MySqlModel(User, pool,
{
    table: 'user',
    fields: { 
        id: {field: 'id', id: true},
        name: {field: 'name'}
    }
});

// Configuration

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'jade');
  app.use(express.profiler());
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(connected_model.connectedModel('/users', User, 'User'));
  app.use(app.router);
  app.use(express.static(__dirname + '/public'));
  app.use(express.errorHandler({dumpExceptions: true}));
});

// Routes
app.get('/', function(req, res){
  res.render('index', {
    title: 'Express'
  });
});

app.listen(3000);
console.log("Express server listening on port %d", app.address().port);
