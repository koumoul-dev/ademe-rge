const fs = require('fs')
fs.writeFileSync('codes_qualification_inconnus.csv', 'code qualification;nombre de sirets\n'+require('./unknown_codes.json').aggs.map(a => a.value+';'+a.total).join('\n'))
