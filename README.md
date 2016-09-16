# nedbclient
Client package for lookatsrc/nedbserver

## Install

`npm install lookatsrc/nedbclient --save`

## Example

`var nedbclient = require('nedbclient');`

`var collection = nedbclient.loadCollection(9000, 'documents');`

`collection.insert({name: 'something'}, function(e, data){});`
