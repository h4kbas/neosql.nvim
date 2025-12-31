local ExportManager = {}
ExportManager.__index = ExportManager

function ExportManager.new()
  local self = setmetatable({}, ExportManager)
  return self
end

function ExportManager:_ensure_columns(data, columns)
  if columns and #columns > 0 then
    return columns
  end

  if not data or #data == 0 then
    return {}
  end

  columns = {}
  for key, _ in pairs(data[1]) do
    table.insert(columns, key)
  end

  return columns
end

function ExportManager:_ensure_directory(filepath)
  local dir = vim.fn.fnamemodify(filepath, ":h")
  if dir and dir ~= "" and dir ~= "." then
    vim.fn.mkdir(dir, "p")
  end
end

function ExportManager:export_to_csv(data, columns, filepath)
  columns = self:_ensure_columns(data, columns)
  if #columns == 0 then
    return false, "No columns to export"
  end

  self:_ensure_directory(filepath)

  local lines = {}
  table.insert(lines, table.concat(columns, ","))

  for _, row in ipairs(data) do
    local values = {}
    for _, col in ipairs(columns) do
      local value = tostring(row[col] or "")
      value = string.gsub(value, '"', '""')
      if string.find(value, ",") or string.find(value, '"') or string.find(value, "\n") then
        value = '"' .. value .. '"'
      end
      table.insert(values, value)
    end
    table.insert(lines, table.concat(values, ","))
  end

  local content = table.concat(lines, "\n")
  local file = io.open(filepath, "w")
  if not file then
    return false, "Failed to open file for writing"
  end

  file:write(content)
  file:close()
  return true, nil
end

function ExportManager:export_to_json(data, filepath)
  if not data then
    return false, "No data to export"
  end

  self:_ensure_directory(filepath)

  local json = vim.json.encode(data)
  local file = io.open(filepath, "w")
  if not file then
    return false, "Failed to open file for writing"
  end

  file:write(json)
  file:close()
  return true, nil
end

function ExportManager:export(data, columns, filepath)
  if not filepath or filepath == "" then
    return false, "No filepath provided"
  end

  filepath = vim.fn.fnamemodify(filepath, ":p")

  local extension = filepath:match("%.(%w+)$")
  if not extension then
    return false, "Could not determine format from file extension"
  end

  extension = extension:lower()

  if extension == "csv" then
    return self:export_to_csv(data, columns, filepath)
  elseif extension == "json" then
    return self:export_to_json(data, filepath)
  else
    return false, "Unsupported format: " .. extension .. ". Supported formats: csv, json"
  end
end

return ExportManager

