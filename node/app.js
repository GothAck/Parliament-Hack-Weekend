
/**
 * Module dependencies.
 */

var express = require('express');
var FastLegS = require('FastLegS');

// FastLegS Config

var dbParams = {
  user:     'greg',
  password: 'greg',
  database: 'greg',
  host:     'localhost',
  port:     5432
}

FastLegS.connect(dbParams);

var PersonWord, Person, Word;

PersonWord = FastLegS.Base.extend({
  tableName: 'person_word',
  primaryKey: ['person_id', 'related_word_id'],
//  belongsTo: [
//    {'word': Word, joinOn: 'word.related_word_id'}
//  ],

//  belongsTo: [
//    {'person': 'person', joinOn: 'person_id'}
//  ]
});

Person = FastLegS.Base.extend({
  tableName: 'person',
  primaryKey: 'id',
  many: [
    {'words': PersonWord, joinOn: 'person_id'}
  ],
  words: function () {
    console.log(this);
  }
});

Word = FastLegS.Base.extend({
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

app.get('/person/search/:person_name', function(req, res){
  res.render('people', {
    people: req.People,
  });
});

app.get('/person/:person_id', function(req, res){
  res.render('person', {
    person: req.Person,
  });
});

app.get('/person', function(req, res, next){
  if (req.query.person_id)
    res.redirect('/person/'+req.query.person_id);
  if (req.query.person_name)
    res.redirect('/person/search/'+req.query.person_name);
  next();
});

app.get('/word/search/:word_string', function(req, res){
  res.render('words', {
    words: req.Words,
  });
});

app.get('/word/:word_id', function(req, res){
  res.render('word', {
    word: req.Word,
  });
});

app.get('/word', function(req, res, next){
  if (req.query.word_id)
    res.redirect('/word/'+req.query.word_id);
  if (req.query.word_string)
    res.redirect('/word/search/'+req.query.word_string);
  next();
});

app.param('person_id', function(req, res, next, id){

//.find(1, {include:{people:{include:{word:{}}}}}, function (e,r) {console.log(r)});

  Person.find(
    id,
    { include: { words: { include: { word: {} } } } },
    function (err, result) {
      if (err)
        throw new Error();
      req.Person = result;
      console.log(JSON.stringify(result));
      next();
    }
  );
});

app.param('person_name', function(req, res, next, id){
  console.log('searching for person_name', id);
  Person.find({'canonical_name.ilike': '%'+id+'%'}, function (err, result) {
    if (err)
      throw new Error();
    req.People = result;
    console.log(JSON.stringify(result));
    next();
  });
});

app.param('word_id', function(req, res, next, id){
  Word.find(
    id,
    { include: { people: { include: { person: {} } } } },
    function (err, result) {
      if (err)
        throw new Error();
      req.Word = result;
      next();
    }
  );
});

app.param('word_string', function(req, res, next, id){
  Word.find({'canonical_name.ilike': '%'+id+'%'}, function (err, result) {
    if (err)
      next(err);
    req.Words = result;
    next();
  });
});

app.listen(3001);
console.log("Express server listening on port %d in %s mode", app.address().port, app.settings.env);

//var repl = require('repl');

//repl.start().context.app = Person;
