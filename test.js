var dbclient = require(__dirname);
var db = dbclient.loadCollection(9000, 'items');

function run(i) {
  if (i < 0) {
    return;
  }
  
  i--;
  db.insert({name:'Google'}, function(err, data) {
    console.log({err:err, data:data});
    process.nextTick(run, i);
  });
}

//run(10000);

db.count({}, function(err, count) {
  console.log({err:err, count:count});
})