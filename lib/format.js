const moment = require('moment')

// day is a basic concatenated date in original data, always process it as a ISO date
exports.day2date = (day) => day.slice(0, 4) + '-' + day.slice(4, 6) + '-' + day.slice(6, 8)

// numbers on data use commas
exports.parseNumber = (str) => str ? Number(str.replace(',', '.')) : undefined

// the short int notation is used for efficient storage of dates in pbf
exports.int2day = (i) => i ? moment('2000-01-01').add(i, 'days').format('YYYY-MM-DD') : undefined
exports.day2int = (day) => moment(day, 'YYYY-MM-DD').diff(moment('2000-01-01'), 'days')
