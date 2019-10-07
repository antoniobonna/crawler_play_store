var gplay = require('/home/postgres/node_modules/google-play-scraper');
const util = require('util');

util.inspect.defaultOptions.maxArrayLength = null;

gplay.reviews({
   appId: '{}',
   num: 1000,
   lang: 'pt_BR',
   sort: gplay.sort.NEWEST
}).then(console.dir,console.dir)

