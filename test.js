var dbclient = require(__dirname);
var database = dbclient.newDatabase(9000);

var db = database.loadCollection('count-table');

/*
db.count({}, function(err, count) {
  console.log(err);
  console.log(count);
});
*/


db.ensureIndex({fieldName: 'name', unique: true}, function (err) {
  if (err) {
    console.log('err in ensure');
    console.log(err);
    return;
  }
  
  db.insert([{
    name: 'Google5'
  }, {
    name: 'Microsoft5'
  }
  ], function(err, data) { 
    console.log(err);
    console.log(data);
  });
});




/*db.insert({
  name: 'Google5'
});*/