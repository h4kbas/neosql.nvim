local ResultManager = {}
ResultManager.__index = ResultManager

function ResultManager.new()
  local self = setmetatable({}, ResultManager)
  return self
end

function ResultManager:format_result(data, columns)
  return self:format_result_with_changes(data, columns, nil)
end

function ResultManager:format_result_with_changes(data, columns, changes)
  if not data or type(data) ~= "table" or #data == 0 then
    return { "No results" }
  end

  -- Verify data structure
  if type(data[1]) ~= "table" then
    return { "Invalid data format: expected array of objects" }
  end

  columns = self:_ensure_columns(data, columns)
  if not columns or #columns == 0 then
    return { "No columns found in data" }
  end

  local change_map = self:_build_change_map(changes)
  local col_widths = self:_calculate_column_widths(data, columns, change_map)
  local formatted = {}
  
  table.insert(formatted, self:_format_header(columns, col_widths))
  table.insert(formatted, self:_format_separator(columns, col_widths))
  
  for i, row in ipairs(data) do
    if type(row) == "table" then
      local change_info = change_map[i]
      local is_deleted = change_info and change_info.status == "deleted"
      local display_row = self:_apply_changes_to_row(row, change_info)
      table.insert(formatted, self:_format_data_row(display_row, columns, col_widths, change_info, is_deleted))
    end
  end

  return formatted
end

function ResultManager:_ensure_columns(data, columns)
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

function ResultManager:_calculate_column_widths(data, columns, change_map)
  local col_widths = {}
  
  for _, col in ipairs(columns) do
    col_widths[col] = string.len(tostring(col))
  end
  
  for i, row in ipairs(data) do
    local display_row = row
    if change_map and change_map[i] then
      display_row = self:_apply_changes_to_row(row, change_map[i])
    end
    
    for _, col in ipairs(columns) do
      local value_str = self:_value_to_string(display_row[col])
      local value_len = string.len(value_str)
      
      if value_len > col_widths[col] then
        col_widths[col] = value_len
      end
    end
  end
  
  for _, col in ipairs(columns) do
    col_widths[col] = col_widths[col] + 2
  end
  
  return col_widths
end

function ResultManager:_value_to_string(value)
  if value == nil then
    return "NULL"
  end
  return tostring(value)
end

function ResultManager:_format_cell(value, width, is_changed)
  local value_str = self:_value_to_string(value)
  local value_len = string.len(value_str)
  
  if value_len > width - 2 then
    value_str = value_str:sub(1, width - 2)
    value_len = width - 2
  end
  
  local trailing_spaces = width - 1 - value_len
  
  return " " .. value_str .. string.rep(" ", trailing_spaces)
end

function ResultManager:_format_header(columns, col_widths)
  local header_parts = {}
  for _, col in ipairs(columns) do
    table.insert(header_parts, self:_format_cell(col, col_widths[col], false))
  end
  return "|" .. table.concat(header_parts, "|") .. "|"
end

function ResultManager:_format_separator(columns, col_widths)
  local separator_parts = {}
  for _, col in ipairs(columns) do
    table.insert(separator_parts, string.rep("-", col_widths[col]))
  end
  return "|" .. table.concat(separator_parts, "|") .. "|"
end

function ResultManager:_format_data_row(row, columns, col_widths, change_info, is_deleted)
  local row_parts = {}
  for _, col in ipairs(columns) do
    local is_changed = change_info and change_info.changes[col] ~= nil
    table.insert(row_parts, self:_format_cell(row[col], col_widths[col], is_changed))
  end
  return "|" .. table.concat(row_parts, "|") .. "|"
end

function ResultManager:_build_change_map(changes)
  local change_map = {}
  if changes then
    for row_idx, change in pairs(changes) do
      change_map[change.row_index] = change
    end
  end
  return change_map
end

function ResultManager:_apply_changes_to_row(row, change_info)
  if not change_info then
    return row
  end

  local display_row = {}
  for key, value in pairs(row) do
    display_row[key] = value
  end

  if change_info.changes then
    for field, value in pairs(change_info.changes) do
      display_row[field] = value
    end
  end

  return display_row
end

return ResultManager

