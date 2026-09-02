const moment = require('moment')

// day is a basic concatenated date in original data, always process it as a ISO date
exports.day2date = (day) => day.slice(0, 4) + '-' + day.slice(4, 6) + '-' + day.slice(6, 8)
exports.date2day = (date) => moment(date).format('YYYYMMDD')

// numbers on data use commas
exports.parseNumber = (str) => str ? Number(str.replace(',', '.')) : undefined

// the short int notation is used for efficient storage of dates in pbf.
// int2day / day2int are called once per record of the daily diff while there are only a
// few thousand distinct days : memoizing avoids building 2 moment objects per record
const int2dayCache = new Map()
exports.int2day = (i) => {
  if (!i) return undefined
  let day = int2dayCache.get(i)
  if (day === undefined) {
    day = moment('2000-01-01').add(i, 'days').format('YYYY-MM-DD')
    int2dayCache.set(i, day)
  }
  return day
}

const day2intCache = new Map()
exports.day2int = (day) => {
  let i = day2intCache.get(day)
  if (i === undefined) {
    i = moment(day, 'YYYY-MM-DD').diff(moment('2000-01-01'), 'days')
    day2intCache.set(day, i)
  }
  return i
}
