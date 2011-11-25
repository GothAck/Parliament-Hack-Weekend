
process.on('uncaughtException', function (err) {
  console.log('uncaughtException', err, err.stack);
  console.trace();
});

/**
 * Module dependencies.
 */

var express = require('express');
var FastLegS = require('FastLegS');
var fs = require('fs');

// Express app creation

var app = module.exports = express.createServer();

var logFile = fs.createWriteStream('./logs/express.log', { flags: 'a+'});

// FastLegS Config

var user = (app.settings.env=='production') ? 'greg' : process.env['USER'];
var dbParams = {
  user:     user,
  password: user,
  database: user,
  host:     'localhost',
  port:     5432
}

FastLegS.connect(dbParams);

// FastLegS Table definitions

var Input = FastLegS.Base.extend({
  tableName: 'input',
});

var PersonWord = FastLegS.Base.extend({
  tableName: 'person_word_proportional',
  primaryKey: ['person_id', 'related_word_id'],
});

var WordDistance = FastLegS.Base.extend({
  tableName: 'word_word_distance',
  primaryKey: ['word_id', 'related_word_id'],
});

var Person = FastLegS.Base.extend({
  tableName: 'person',
  primaryKey: 'id',
  many: [
    {'words': PersonWord, joinOn: 'person_id'}
  ],
  words: function () {
    console.log(this);
  }
});

var Word = FastLegS.Base.extend({
  tableName: 'word',
  primaryKey: 'id',
  many: [
    {'people': PersonWord, joinOn: 'related_word_id'}
  ]
});

PersonWord.one = [
  {'person': Person, joinOn: 'person_id'},
  {'word': Word, joinOn: 'related_word_id'}
]

// Custom Errors

function NotFound(msg) {
  this.name="NotFound";
  Error.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
}
function InvalidParam(msg) {
  this.name="InvalidParam";
  Error.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
}

NotFound.prototype.__proto__ = Error.prototype;

// Configuration

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'html');
  app.register('.html', require('jqtpl').express);
  app.use(express.logger({stream: logFile}));
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(express.cookieParser());
//  app.use(express.session({ secret: 'ajfihuc43fu34uxmiqe4fdjwefu3u4ure' }));
//  app.use(express.compiler({ src: __dirname + '/public', enable: ['sass'] }));
// GregM: Custom Middleware to set req.obs object & populate with query string items
  app.use(function (req, res, next) {
    if (!req.obs) req.obs = {};
    req.obs.query = req.query;
    next();
  });
  app.use(app.router);
  app.use(express.static(__dirname + '/public'));
// GregM: If the middleware processing gets to here then nothing has been found in routing & static, raise the NotFound error.
  app.use(function(req, res, next) {
    next(new NotFound);
  });
});

app.configure('development', function(){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});

app.configure('production', function(){
  // To catch our custom errors we need to use our own error handler
  app.error(function(err, req, res, next) {
    if (err instanceof NotFound) {
      res.render('404.html', {status:404});
    } else if (err instanceof InvalidParam) {
      res.render('403.html', {status:403});
    } else {
      res.render('500.html', {status:500});
    }
  });
});

// Routes

app.get('/', function(req, res){
  res.render('index', {
    title: 'Express'
  });
});

app.get('/about', function(req, res){
  res.render('about', {});
});

app.get('/about/presentation', function(req, res){
  res.render('presentation', {});
});

app.get('/person/search/:person_name.:format?', function(req, res){
  res.render('people', {
    people: req.obs.People,
  });
});

app.get('/person/:person_id.:format?', function(req, res){
  var min = 9001;
  var max = 0;
  req.obs.Person.words.forEach(function(f) {
  	min = Math.min(min, f.uses);
  	max = Math.max(max, f.uses);
  });
  res.render('person', {
    person: req.obs.Person,
	min_uses: min,
	max_uses: max,
  });
});

app.get('/person.:formatr?', function(req, res, next){
  var format = (req.params.formatr)?'.'+req.params.formatr:'';
  if ('person_id' in req.query) {
    res.redirect('/person/'+req.query.person_id+format);
  } else if ('person_name' in req.query) {
    res.redirect('/person/search/'+req.query.person_name+format);
  } else {
    res.redirect('/person/search/_'+format);
  }
  next();
});

app.get('/word/search/:word_string.:format?', function(req, res){
  res.render('words', {
    words: req.obs.Words,
  });
});

app.get('/word/:word_id.:format?', function(req, res){
  var min = 9001;
  var max = 0;
  req.obs.Word.people.forEach(function(f) {
  	min = Math.min(min, f.uses);
  	max = Math.max(max, f.uses);
  });
  res.render('word', {
    word: req.obs.Word,
    distance: req.obs.WordDistance,
	  min_uses: min,
	  max_uses: max,
  });
});

app.get('/word.:formatr?', function(req, res, next){
  var format = (req.params.formatr)?'.'+req.params.formatr:'';
  if ('word_id' in req.query) {
    res.redirect('/word/'+req.query.word_id+format);
  } else if ('word_string' in req.query) {
    res.redirect('/word/search/'+req.query.word_string+format);
  } else {
    res.redirect('/word/search/_'+format);
  }
  next();
});

app.get('/references/:person_id/:reference_word.:format?', function(req, res){
    res.render('references', {
      name: req.obs.Person.name,
      word: req.params.reference_word,
      references: req.obs.references,
    });
});

app.param('reference_word', function(req, res, next, id){
  if (typeof id != 'string' || id.length > 100)
    return next(new InvalidParam);
  var query = "SELECT url, ts_headline(text, to_tsquery($1)), timestamp FROM input WHERE speakerid=$2 AND to_tsvector(text) @@ to_tsquery($1)";
  console.log(query, [id, req.obs.Person.id]);
  query = FastLegS.client.client.query(query, [id, req.obs.Person.id], function(err, results) {
    console.log('reference_word', err, results);
    if (err) return next(err);
    if (!results) return next(new NotFound);
    req.obs.references = results.rows;
    next();
  });
});

app.param('person_id', function(req, res, next, id){
  if (isNaN(id) || id < 1)
    return next(new InvalidParam);
  Person.findOne(
    id,
    { include: { words: { limit: req.query.limit || 50, offset: req.query.offset || 0, order: ['-uses'], include: { word: {} } } } },
    function (err, result) {
      console.log('rar', result);
      if (err) return next(err);
      if (!result) return next(new NotFound);
      if (result.words) result.words.sort(function(a, b) {return a.word.name > b.word.name ? 1 : -1;});
      req.obs.Person = result;
      next();
    }
  );
});

app.param('person_name', function(req, res, next, id){
  if (typeof id != 'string' || id.length > 100)
    return next(new InvalidParam);
  console.log('searching for person_name', id);
  if (id) id = '%' + id + '%';
  if (!id) id = '%';
  Person.find({'name.ilike': id}, { limit: req.query.limit || 20, offset: req.query.offset || 0 }, function (err, result) {
    if (err) return next(err);
    req.obs.People = result;
    //console.log(JSON.stringify(result));
    next();
  });
});

app.param('word_id', function(req, res, next, id){
  if (isNaN(id) || id < 1)
    return next(new InvalidParam);
  Word.findOne(
    id,
    { include: { people: { limit: req.query.limit || 20, offset: req.query.offset || 0, order: ['-uses_proportion'], include: { person: {} } } } },
    function (err, result) {
      if (err) return next(err);
      if (! result) return next(new NotFound);
	    if (result.people) result.people.sort(function(a, b) {return a.person.name > b.person.name ? 1 : -1;});
      req.obs.Word = result;
      var query = "SELECT *, (SELECT name from word where id=word_id) as word_name, (SELECT name FROM word WHERE id=related_word_id) as related_word_name, ((6-dist)*(6-dist))*uses as b3 FROM word_word_distance WHERE dist < 5 AND (word_id = $1 OR related_word_id = $1) ORDER BY b3 LIMIT 15";
      query = FastLegS.client.client.query(query, [result.id]);
      query.on('error', function (err) {
        next(err);
      });
      var result = [];
      query.on('row', function (item) {
        result.push({
          word: (item.related_word_id == req.obs.Word.id) ? item.word_name : item.related_word_name,
          id:   (item.related_word_id == req.obs.Word.id) ? item.word_id : item.related_word_id,
          rel:  item.b3,
          dist: item.dist,
          uses: item.uses,
        });
      });
      query.on('end', function () {
        req.obs.WordDistance = result;
        next();
      });
    }
  );
});

app.param('word_string', function(req, res, next, id){
  if (typeof id != 'string' || id.length > 100)
    return next(new InvalidParam);
  if (id) id = '%' + id + '%';
  if (!id) id = '%';
  Word.find({'name.ilike': '%'+id+'%'}, { limit: req.query.limit || 20, offset: req.query.offset || 0 }, function (err, result) {
    if (err)
      next(err);
    req.obs.Words = result;
    next();
  });
});

app.param('format', function(req, res, next, id){
  if (typeof id != 'string' )
    return next(new InvalidParam);
  if (id == 'json') {
    res.json(req.obs);
    res.end();
    return;
  }
  console.log('default');
  next();
});

if (app.settings.env == 'production') {
  app.listen(80, '31.3.241.106');
} else {
  app.listen(3001);
}
console.log("Express server listening on port %d in %s mode", app.address().port, app.settings.env);

//var repl = require('repl');

//var c = repl.start();
//c.context.FastLegS = FastLegS ;
//c.context.Word = Word;
