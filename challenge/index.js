const csv = require('csv-parser')
const fs = require('fs')
const lodash = require('lodash')
const { concat, trim, pick } = lodash

const INPUT_PATH = '/index.csv'
const OUTPUT_PATH = '/output.json'

const result = []
const headers = []
const mapHeaders = ({ header, index }) => {
  headers.push(header)
  return `col${index}`
}

const possibleAddress = ['email', 'phone']
const isAddress = column => {
  const tags = column.split(' ')
  const type = tags.shift()
  if (!possibleAddress.includes(type)) {
    return false
  }
  return { tags, type }
}
const handleAddress = columnName => data => {
  const address = isAddress(columnName)
  if (!address) {
    return data
  }
  return {
    ...pick(address, ['tags', 'type']),
    address: data
  }
}

const handleColumn = (columnName, data) =>
  ((
    {
      group: data => data.split(/[^\w\s]+/).map(trim)
    }[columnName] || handleAddress(columnName)
  )(data))

const merge = (source, target) => {
  Object.keys(target).forEach(key => {
    if (Array.isArray(target[key])) {
      target[key] = concat(source[key], target[key])
    }
  })
  return target
}

const parse = (accumulator, reg) => {
  const object = {}
  reg.forEach((element, index) => {
    if (!element) {
      return
    }
    let header = headers[index]
    const newElement = handleColumn(header, element)
    if (isAddress(header)) {
      header = 'addresses'
    }
    if (object[header] == null) {
      object[header] = newElement
    } else {
      object[header] = concat(object[header], newElement)
    }
  })
  const indexId = headers.findIndex(h => h === 'eid')
  const id = reg[indexId]
  const finded = accumulator.findIndex(o => o.eid === id)
  if (finded != -1) {
    accumulator[finded] = merge(object, accumulator[finded])
  } else {
    accumulator.push(object)
  }
  return accumulator
}

fs.createReadStream(`${__dirname}${INPUT_PATH}`)
  .pipe(csv({ mapHeaders }))
  .on('data', data => result.push(data))
  .on('end', () => {
    const parsed = result.map(Object.values).reduce(parse, [])
    fs.writeFileSync(
      `${__dirname}${OUTPUT_PATH}`,
      JSON.stringify(parsed, null, 2)
    )
  })
