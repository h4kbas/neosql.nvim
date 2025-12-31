local Postgres = require('neosql.postgres')
local WindowManager = require('neosql.window_manager')
local DataManager = require('neosql.data_manager')
local ResultManager = require('neosql.result_manager')
local ExportManager = require('neosql.export_manager')
local BindingManager = require('neosql.binding_manager')
local Commands = require('neosql.commands')

local AppManager = {}
AppManager.__index = AppManager

function AppManager.new()
  local self = setmetatable({}, AppManager)
  self.db = nil
  self.window_manager = WindowManager.new()
  self.data_manager = DataManager.new()
  self.result_manager = ResultManager.new()
  self.export_manager = ExportManager.new()
  self.binding_manager = nil
  self.commands = nil
  self.is_open = false
  return self
end

function AppManager:setup_bindings(binding_opts)
  self.binding_manager = BindingManager.new(self)
  self.binding_manager:setup(binding_opts)
end

function AppManager:setup_commands()
  self.commands = Commands.new(self)
  self.commands:register()
end

function AppManager:connect(config)
  if self.db and self.db:is_connected() then
    self.db:disconnect()
  end

  self.db = Postgres.new(config)
  local ok, err = pcall(function()
    self.db:connect()
  end)

  if not ok then
    return false, err
  end

  return true, nil
end

function AppManager:disconnect()
  if self.db and self.db:is_connected() then
    self.db:disconnect()
  end
  self.db = nil
end

function AppManager:open()
  if self.is_open then
    return
  end

  if not self.db or not self.db:is_connected() then
    error("Not connected to database. Call connect() first.")
  end

  self.window_manager:create()
  
  if self.binding_manager then
    self.binding_manager:register_all_bindings(self.window_manager)
  end
  
  self.is_open = true
  self:load_table_list()
end

function AppManager:close()
  if not self.is_open then
    return
  end

  self.window_manager:close()
  self.data_manager:clear()
  self.is_open = false
end

function AppManager:load_table_list()
  if not self.db or not self.db:is_connected() then
    return
  end

  if not self.db.get_tables then
    self.window_manager:set_table_list({ "Database driver does not support table listing" })
    return
  end

  local tables, err = self.db:get_tables()
  if err then
    self.window_manager:set_table_list({ "Error loading tables: " .. tostring(err) })
    return
  end

  if not tables or #tables == 0 then
    tables = { "No tables found" }
  end

  self.window_manager:set_table_list(tables)
end

function AppManager:execute_query()
  if not self.db or not self.db:is_connected() then
    self.window_manager:set_result({ "Error: Not connected to database" })
    return false, "Not connected to database"
  end

  local query = self.window_manager:get_query()
  if not query or query:match("^%s*$") then
    self.window_manager:set_result({ "No query to execute" })
    return false, "No query"
  end

  local ok, result = pcall(function()
    return self.db:query(query)
  end)

  if not ok then
    local error_msg = tostring(result)
    self.window_manager:set_result({ "Query error: " .. error_msg })
    return false, error_msg
  end

  if not result then
    self.window_manager:set_result({ "Query executed successfully (no results)" })
    self.data_manager:clear()
    return true, nil
  end

  local rows = result
  local columns = nil
  if result and result.rows then
    rows = result.rows
    columns = result.columns
  end

  if type(rows) ~= "table" or #rows == 0 then
    self.window_manager:set_result({ "No results returned" })
    self.data_manager:clear()
    return true, nil
  end

  if type(rows[1]) ~= "table" then
    self.window_manager:set_result({ "Unexpected result format", tostring(result) })
    return false, "Unexpected result format"
  end

  local primary_keys = {}
  if self.db and self.db.get_primary_keys then
    local table_name, was_quoted = self:_get_table_name_from_query()
    if table_name then
      primary_keys = self.db:get_primary_keys(table_name, nil, was_quoted) or {}
    end
  end

  self.data_manager:set_data(rows, primary_keys, columns)

  local extracted_columns = self.data_manager:get_columns()
  local changes = self.data_manager:get_all_changes()
  
  if not extracted_columns or #extracted_columns == 0 then
    self.window_manager:set_result({ "No columns found in result" })
    return false, "No columns found"
  end

  local formatted = self.result_manager:format_result_with_changes(rows, extracted_columns, changes)

  if not formatted or #formatted == 0 then
    self.window_manager:set_result({ "Failed to format result" })
    return false, "Failed to format result"
  end

  self.window_manager:set_result(formatted, self.data_manager)
  return true, nil
end

function AppManager:add_change(row_index, field, value)
  self.data_manager:add_change(row_index, field, value)
  self:refresh_result()
end

function AppManager:remove_change(row_index)
  self.data_manager:remove_change(row_index)
  self:refresh_result()
end

function AppManager:remove_cell_change(row_index, field)
  self.data_manager:remove_cell_change(row_index, field)
  self:refresh_result()
end

function AppManager:undo_row_changes(row_index)
  self.data_manager:undo_row_changes(row_index)
  self:refresh_result()
end

function AppManager:undo_table_changes()
  self.data_manager:undo_table_changes()
  self:refresh_result()
end

function AppManager:refresh_result()
  local data = self.data_manager:get_data()
  local columns = self.data_manager:get_columns()
  local changes = self.data_manager:get_all_changes()

  local formatted = self.result_manager:format_result_with_changes(data, columns, changes)
  self.window_manager:set_result(formatted, self.data_manager)
end

function AppManager:apply_changes()
  if not self.data_manager:has_changes() then
    return false, "No changes to apply"
  end

  if not self.db or not self.db:is_connected() then
    return false, "Not connected to database"
  end

  local table_name, was_quoted = self:_get_table_name_from_query()
  if not table_name then
    return false, "Could not determine table name"
  end

  local primary_keys = self.data_manager.primary_keys
  if #primary_keys == 0 then
    if self.db and self.db.get_primary_keys then
      primary_keys = self.db:get_primary_keys(table_name, nil, was_quoted) or {}
      if #primary_keys > 0 then
        self.data_manager.primary_keys = primary_keys
      end
    end
  end

  if #primary_keys == 0 then
    return false, "No primary keys detected for table '" .. table_name .. "'. Cannot apply changes."
  end

  local updates = self.data_manager:get_changes_for_update()
  
  if not self.db.apply_updates then
    return false, "Database does not support applying updates"
  end

  local ok, err = self.db:apply_updates(table_name, updates)
  if not ok then
    return false, err
  end

  self.data_manager:undo_table_changes()
  self:execute_query()
  return true, nil
end

function AppManager:save_result(format, filepath)
  local data = self.data_manager:get_data()
  local columns = self.data_manager:get_columns()

  if not data or #data == 0 then
    return false, "No data to save"
  end

  filepath = vim.fn.fnamemodify(filepath, ":p")

  if format == "csv" then
      return self.export_manager:export_to_csv(data, columns, filepath)
    elseif format == "json" then
      return self.export_manager:export_to_json(data, filepath)
  else
    return false, "Unknown format: " .. tostring(format) .. ". Supported formats: csv, json"
  end
end

function AppManager:export(filepath)
  local data = self.data_manager:get_data()
  local columns = self.data_manager:get_columns()

  if not data or #data == 0 then
    return false, "No data to export"
  end

  return self.export_manager:export(data, columns, filepath)
end

function AppManager:_get_table_name_from_query()
  local query = self.window_manager:get_query()
  if not query or query:match("^%s*$") then
    return nil, false
  end

  query = query:gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")

  if self.db and self.db.extract_table_name then
    local table_name, was_quoted = self.db:extract_table_name(query)
    if table_name then
      return table_name, was_quoted
    end
  end

  local quoted_patterns = {
    'from%s+"([^"]+)"',
    'update%s+"([^"]+)"',
    'into%s+"([^"]+)"',
  }

  for _, pattern in ipairs(quoted_patterns) do
    local table_name = query:match(pattern)
    if table_name then
      return table_name, true
    end
    table_name = string.lower(query):match(pattern)
    if table_name then
      return table_name, true
    end
  end

  local lower_query = string.lower(query)
  local unquoted_patterns = {
    "from%s+([%w_]+)",
    "update%s+([%w_]+)",
    "into%s+([%w_]+)",
  }

  for _, pattern in ipairs(unquoted_patterns) do
    local table_name = string.match(lower_query, pattern)
    if table_name then
      return table_name, false
    end
  end

  return nil, false
end

function AppManager:is_connected()
  return self.db ~= nil and self.db:is_connected()
end

return AppManager

