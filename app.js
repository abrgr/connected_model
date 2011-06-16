
/**
 * Module dependencies.
 */

var express = require('express');
var hamljs = require('hamljs');
var expose = require('express-expose');
var User = require('./models/user');
var connected_model = require('./connected_model');

var app = module.exports = express.createServer();

// Configuration

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'haml');
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(app.router);
  app.use(express.static(__dirname + '/public'));
});

app.configure('development', function(){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});

app.configure('production', function(){
  app.use(express.errorHandler()); 
});

// Models
app.connectModel('/users', User, 'User');

// Routes
app.get('/', function(req, res){
  res.render('index', {
    title: 'Express'
  });
});

app.listen(3000);
console.log("Express server listening on port %d", app.address().port);
