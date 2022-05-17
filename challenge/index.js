const csv = require('csv-parser')
const fs = require('fs')
const lodash = require('lodash')
const { concat, trim, uniq } = lodash
const { PhoneNumberFormat, PhoneNumberUtil } = require('google-libphonenumber')

const INPUT_PATH = '/index.csv'
const OUTPUT_PATH = '/output.json'

const isAddress = column => {
  const possibleAddresses = ['email', 'phone']
  const tags = column.split(' ')
  const type = tags.shift()
  if (!possibleAddresses.includes(type)) {
    return false
  }
  return { tags, type }
}

const handleAddress = columnName => data => {
  const address = isAddress(columnName)
  if (!address) {
    return data
  }
  return handleColumn(address.type, { addressInfo: address, data })
}

const stringToBoolean = data =>
  ({
    no: false,
    yes: true,
    1: true,
    0: false
  }[data])

const findDividerRegex = /\s*[^\w\s]+\s*/
const splitGroups = data => trim(data).split(findDividerRegex)

const findEmailRegex = /([\w.-]+@[\w.-]+\.[\w-]+)/gi
const parseEmail = ({ addressInfo, data }) =>
  data.match(findEmailRegex)?.map(email => ({ ...addressInfo, address: email }))

const phoneUtil = PhoneNumberUtil.getInstance()
const parsePhone = ({ addressInfo, data }) => {
  let number
  try {
    number = phoneUtil.parse(data, 'BR')
  } catch (err) {
    return []
  }
  if (!phoneUtil.isValidNumberForRegion(number, 'BR')) {
    return []
  }
  return {
    ...addressInfo,
    address: phoneUtil.format(number, PhoneNumberFormat.E164).replace('+', '')
  }
}

// prettier-ignore
const handleColumn = (columnName, data) => ({
  invisible: stringToBoolean,
  see_all: stringToBoolean,
  group: splitGroups,
  email: parseEmail,
  phone: parsePhone
}[columnName] || handleAddress(columnName))(data)

const renameColumns = currentName =>
  ({
    group: 'groups'
  }[currentName] || currentName)

const insertDefaultValues = obj => {
  const defaultValues = {
    groups: [],
    invisible: false,
    see_all: false
  }
  Object.keys(defaultValues).forEach(key => {
    obj[key] ?? (obj[key] = defaultValues[key])
  })
}

const merge = (source, target) => {
  Object.keys(target).forEach(key => {
    if (Array.isArray(target[key])) {
      target[key] = uniq(concat(source[key], target[key]))
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
    } else {
      header = renameColumns(header)
    }
    if (object[header] == null) {
      object[header] = newElement
    } else {
      object[header] = concat(object[header], newElement)
    }
  })
  insertDefaultValues(object)
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

const result = []
const headers = []
const mapHeaders = ({ header, index }) => {
  headers.push(header)
  return `col${index}`
}
fs.createReadStream(`${__dirname}${INPUT_PATH}`)
  .pipe(csv({ mapHeaders }))
  .on('data', data => result.push(Object.values(data)))
  .on('end', () => {
    const parsed = result.reduce(parse, [])
    fs.writeFileSync(
      `${__dirname}${OUTPUT_PATH}`,
      JSON.stringify(parsed, null, 2)
    )
  })
