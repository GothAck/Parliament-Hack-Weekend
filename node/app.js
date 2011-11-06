
/**
 * Module dependencies.
 */

var express = require('express');
var FastLegS = require('FastLegS');

// FastLegS Config

var user = process.env['USER'];
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

// Express app creation

var app = module.exports = express.createServer();

// Configuration

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'html');
  app.register('.html', require('jqtpl').express);
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(express.cookieParser());
//  app.use(express.session({ secret: 'ajfihuc43fu34uxmiqe4fdjwefu3u4ure' }));
//  app.use(express.compiler({ src: __dirname + '/public', enable: ['sass'] }));
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
    
    // Base return
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
    // Base return
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


// Parameter pre-processing

app.param('reference_word', function(req, res, next, id){
  var query = "SELECT url, ts_headline(text, $1::tsquery), timestamp FROM input WHERE speakerid=$2 AND to_tsvector(text) @@ $1::tsquery";
  query = FastLegS.client.client.query(query, [id, req.obs.Person.id]);

  var results = [];
  query.on('error', function(err) {
  	console.log(err);
  });
  query.on('row', function(row) {
  	results.push(row);
  });
  query.on('end', function() {
    req.obs.references = results;
	next();
  });
});

app.param('person_id', function(req, res, next, id){
  Person.find(
    id,
    { include: { words: { limit: req.query.limit || 50, offset: req.query.offset || 0, order: ['-uses'], include: { word: {} } } } },
    function (err, result) {
      if (err) return next(err);
      if (!req.obs) req.obs = {};
	  result.words.sort(function(a, b) {return a.word.name > b.word.name ? 1 : -1;});
      req.obs.Person = result;
      console.log(JSON.stringify(result));
      next();
    }
  );
});

app.param('person_name', function(req, res, next, id){
  console.log('searching for person_name', id);
  if (id) id = '%' + id + '%';
  if (!id) id = '%';
  Person.find({'name.ilike': id}, { limit: req.query.limit || 20, offset: req.query.offset || 0 }, function (err, result) {
    if (err) return next(err);
    if (!req.obs) req.obs = {};
    req.obs.People = result;
    console.log(JSON.stringify(result));
    next();
  });
});

app.param('word_id', function(req, res, next, id){
  Word.find(
    id,
    { include: { people: { limit: req.query.limit || 20, offset: req.query.offset || 0, order: ['-uses_proportion'], include: { person: {} } } } },
    function (err, result) {
      if (err) return next(err);
      if (!req.obs) req.obs = {};
	  result.people.sort(function(a, b) {return a.person.name > b.person.name ? 1 : -1;});
      req.obs.Word = result;
      WordDistance.find(
        {word_id: id},
        { limit: 10 },
        function (err, result) {
          if (err) return next(err);
          req.obs.WordDistance1 = result
          next();
        }
      );
    }
  );
});

app.param('word_string', function(req, res, next, id){
  if (id) id = '%' + id + '%';
  if (!id) id = '%';
  Word.find({'name.ilike': '%'+id+'%'}, { limit: req.query.limit || 20, offset: req.query.offset || 0 }, function (err, result) {
    if (err)
      next(err);
    if (!req.obs) req.obs = {};
    req.obs.Words = result;
    next();
  });
});

app.param('format', function(req, res, next, id){
console.log(id);
  if (!id) next();
  if (id == 'json') {
    res.write(JSON.stringify(req.obs));
    res.end();
  }
});

app.listen(3001);
console.log("Express server listening on port %d in %s mode", app.address().port, app.settings.env);

//var repl = require('repl');

//repl.start().context.app = Person;
