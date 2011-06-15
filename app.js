
/**
 * Module dependencies.
 */

var express = require('express');
var expose = require('express-expose');
var User = require('./models/user');
var connected_model = require('./connected_model');

var app = module.exports = express.createServer();

// Configuration

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'jade');
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

// Routes

app.get('/', function(req, res){
  res.render('index', {
    title: 'Express'
  });
});

app.connect_model('/users', User, 'User');

app.get('/test', function(req, res) {
    var user = new User(1, 'adam');
    res.send(user.is_valid(), {'content-type': 'text/javascript'});
});

app.listen(3000);
console.log("Express server listening on port %d", app.address().port);
