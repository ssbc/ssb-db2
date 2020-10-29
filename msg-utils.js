function isString (s) {
  return typeof s === 'string'
}

exports.originalValue = function(value) {
  var copy = {}

  for (let key in value) {
    if (key !== 'meta' && key !== 'cyphertext' && key !== 'private' && key !== 'unbox') {
      copy[key] = value[key]
    }
  }

  if (value.meta && value.meta.original) {
    for (let key in value.meta.original) {
      copy[key] = value.meta.original[key]
    }
  }

  return copy
}

exports.originalData = function(data) {
  data.value = exports.originalValue(data.value)
  return data
}
