local BindingManager = {}
BindingManager.__index = BindingManager

function BindingManager.new(app_manager)
  local self = setmetatable({}, BindingManager)
  self.app_manager = app_manager
  self.default_bindings = {
    query = {
      execute = "<CR>",
      focus_result = "e",
      focus_table_list = "t",
    },
    result = {
      edit_cell = "e",
      apply_changes = "a",
      undo_table_changes = "c",
      undo_cell_change = "u",
      undo_row_changes = "U",
      export = "s",
      focus_query = "q",
      focus_table_list = "t",
    },
    table_list = {
      select_table = "<CR>",
      insert_template = "i",
      select_template = "s",
      update_template = "u",
      delete_template = "d",
      focus_query = "q",
      focus_result = "e",
    },
  }
  self.bindings = {}
  self.enabled = true
  return self
end

function BindingManager:setup(opts)
  opts = opts or {}
  
  if opts.enabled == false then
    self.enabled = false
    return
  end

  self.enabled = true
  
  self.bindings = {
    query = vim.tbl_extend("force", self.default_bindings.query, opts.query or {}),
    result = vim.tbl_extend("force", self.default_bindings.result, opts.result or {}),
    table_list = vim.tbl_extend("force", self.default_bindings.table_list, opts.table_list or {}),
  }
end

function BindingManager:register_query_bindings(bufnr)
  if not self.enabled or not self.bindings.query then
    return
  end

  local function execute_query()
    self.app_manager:execute_query()
  end

  local function focus_result()
    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_result()
    end
  end

  local function focus_table_list()
    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_table_list()
    end
  end

  local commands = {
    execute = execute_query,
    focus_result = focus_result,
    focus_table_list = focus_table_list,
  }

  for action, keymap in pairs(self.bindings.query) do
    if commands[action] then
      vim.keymap.set("n", keymap, commands[action], {
        buffer = bufnr,
        desc = "neosql query: " .. action,
        silent = true,
        noremap = true,
      })
    end
  end
end

function BindingManager:register_result_bindings(bufnr)
  if not self.enabled or not self.bindings.result then
    return
  end

  local function edit_cell()
    local result_buf = self.app_manager.window_manager.result_buf
    local result_win = self.app_manager.window_manager.result_win
    
    if not result_buf or not result_win then
      vim.notify("Result buffer or window not found", vim.log.levels.WARN)
      return
    end
    
    if not vim.api.nvim_win_is_valid(result_win) then
      vim.notify("Result window is no longer valid", vim.log.levels.WARN)
      return
    end
    
    local cursor = vim.api.nvim_win_get_cursor(result_win)
    local line_num = cursor[1]
    
    local lines = vim.api.nvim_buf_get_lines(result_buf, 0, -1, false)
    local header_line_idx = nil
    
    for i = 1, #lines do
      local line = lines[i]
      if line and line:match("|") and not line:match("^%s*|%s*%-+") then
        header_line_idx = i
        break
      end
    end
    
    if not header_line_idx then
      vim.notify("Could not find header", vim.log.levels.WARN)
      return
    end
    
    local row = line_num - header_line_idx - 1
    
    if row < 1 then
      vim.notify("Cannot edit header or separator row", vim.log.levels.WARN)
      return
    end

    local data = self.app_manager.data_manager:get_data()
    if not data or row > #data then
      vim.notify(string.format("Invalid row: %d (total rows: %d)", row, #data or 0), vim.log.levels.WARN)
      return
    end

    local columns = self.app_manager.data_manager:get_columns()
    if not columns or #columns == 0 then
      vim.notify("No columns available", vim.log.levels.WARN)
      return
    end
    
    local line = lines[line_num]
    if not line then
      vim.notify("Could not get line from buffer", vim.log.levels.WARN)
      return
    end
    
    local col_index = self:_get_column_index_from_line(line, columns)
    
    if not col_index or col_index < 1 or col_index > #columns then
      vim.notify(string.format("Invalid column index: %d (columns: %d)", col_index or -1, #columns), vim.log.levels.WARN)
      return
    end

    local field = columns[col_index]
    
    if not data[row] then
      vim.notify(string.format("Row %d does not exist in data (total rows: %d)", row, #data), vim.log.levels.WARN)
      return
    end
    
    local row_data = data[row]
    local current_value = tostring(row_data[field] or "")
    
    local change = self.app_manager.data_manager:get_change_by_row_index(row)
    if change and change.changes and change.changes[field] ~= nil then
      current_value = tostring(change.changes[field])
    end
    
    local target_row = row
    local target_field = field
    
    local new_value = vim.fn.input("Edit " .. target_field .. " (row " .. target_row .. "): ", current_value)
    if new_value ~= "" and new_value ~= current_value then
      local parsed_value = self:_parse_value(new_value, data[target_row][target_field])
      self.app_manager:add_change(target_row, target_field, parsed_value)
    end
  end

  local function apply_changes()
    local ok, err = self.app_manager:apply_changes()
    if not ok then
      vim.notify("Failed to apply changes: " .. tostring(err), vim.log.levels.ERROR)
    else
      vim.notify("Changes applied successfully", vim.log.levels.INFO)
    end
  end

  local function undo_table_changes()
    self.app_manager:undo_table_changes()
    vim.notify("Table changes undone", vim.log.levels.INFO)
  end

  local function undo_cell_change()
    local result_buf = self.app_manager.window_manager.result_buf
    local result_win = self.app_manager.window_manager.result_win
    
    if not result_buf or not result_win then
      vim.notify("Result buffer or window not found", vim.log.levels.WARN)
      return
    end
    
    if not vim.api.nvim_win_is_valid(result_win) then
      vim.notify("Result window is no longer valid", vim.log.levels.WARN)
      return
    end
    
    local cursor = vim.api.nvim_win_get_cursor(result_win)
    local line_num = cursor[1]
    
    local lines = vim.api.nvim_buf_get_lines(result_buf, 0, -1, false)
    local header_line_idx = nil
    
    for i = 1, #lines do
      local line = lines[i]
      if line and line:match("|") and not line:match("^%s*|%s*%-+") then
        header_line_idx = i
        break
      end
    end
    
    if not header_line_idx then
      vim.notify("Could not find header", vim.log.levels.WARN)
      return
    end
    
    -- Calculate row index
    local row = line_num - header_line_idx - 1
    
    if row < 1 then
      vim.notify("Cannot undo header or separator row", vim.log.levels.WARN)
      return
    end

    local data = self.app_manager.data_manager:get_data()
    if not data or row > #data then
      vim.notify(string.format("Invalid row: %d (total rows: %d)", row, #data or 0), vim.log.levels.WARN)
      return
    end

    local columns = self.app_manager.data_manager:get_columns()
    if not columns or #columns == 0 then
      vim.notify("No columns available", vim.log.levels.WARN)
      return
    end
    
    local line = lines[line_num]
    if not line then
      vim.notify("Could not get line from buffer", vim.log.levels.WARN)
      return
    end
    
    local col_index = self:_get_column_index_from_line(line, columns)
    
    if not col_index or col_index < 1 or col_index > #columns then
      vim.notify(string.format("Invalid column index: %d (columns: %d)", col_index or -1, #columns), vim.log.levels.WARN)
      return
    end

    local field = columns[col_index]
    
    local change = self.app_manager.data_manager:get_change_by_row_index(row)
    if not change or not change.changes or change.changes[field] == nil then
      vim.notify("No change to undo for this cell", vim.log.levels.INFO)
      return
    end
    
    self.app_manager:remove_cell_change(row, field)
    vim.notify("Cell change undone", vim.log.levels.INFO)
  end

  local function undo_row_changes()
    local result_buf = self.app_manager.window_manager.result_buf
    local result_win = self.app_manager.window_manager.result_win
    
    if not result_buf or not result_win then
      vim.notify("Result buffer or window not found", vim.log.levels.WARN)
      return
    end
    
    if not vim.api.nvim_win_is_valid(result_win) then
      vim.notify("Result window is no longer valid", vim.log.levels.WARN)
      return
    end
    
    local cursor = vim.api.nvim_win_get_cursor(result_win)
    local line_num = cursor[1]
    
    local lines = vim.api.nvim_buf_get_lines(result_buf, 0, -1, false)
    local header_line_idx = nil
    
    for i = 1, #lines do
      local line = lines[i]
      if line and line:match("|") and not line:match("^%s*|%s*%-+") then
        header_line_idx = i
        break
      end
    end
    
    if not header_line_idx then
      vim.notify("Could not find header", vim.log.levels.WARN)
      return
    end
    
    -- Calculate row index
    local row = line_num - header_line_idx - 1
    
    if row < 1 then
      vim.notify("Cannot clear header or separator row", vim.log.levels.WARN)
      return
    end

    local data = self.app_manager.data_manager:get_data()
    if not data or row > #data then
      vim.notify(string.format("Invalid row: %d (total rows: %d)", row, #data or 0), vim.log.levels.WARN)
      return
    end
    
    local change = self.app_manager.data_manager:get_change_by_row_index(row)
    if not change or not change.changes or next(change.changes) == nil then
      vim.notify("No changes to undo for this row", vim.log.levels.INFO)
      return
    end
    
    self.app_manager:undo_row_changes(row)
    vim.notify("Row changes undone", vim.log.levels.INFO)
  end

  local function export()
    local filepath = vim.fn.input("Export to: ", "", "file")
    if filepath and filepath ~= "" then
      local ok, err = self.app_manager:export(filepath)
      if ok then
        local abs_path = vim.fn.fnamemodify(filepath, ":p")
        vim.notify("Exported to " .. abs_path, vim.log.levels.INFO)
      else
        vim.notify("Failed to export: " .. tostring(err), vim.log.levels.ERROR)
      end
    end
  end

  local function export_csv()
    local filepath = vim.fn.input("Save as CSV: ", "", "file")
    if filepath and filepath ~= "" then
      local ok, err = self.app_manager:save_result("csv", filepath)
      if ok then
        local abs_path = vim.fn.fnamemodify(filepath, ":p")
        vim.notify("Exported to " .. abs_path, vim.log.levels.INFO)
      else
        vim.notify("Failed to export: " .. tostring(err), vim.log.levels.ERROR)
      end
    end
  end

  local function export_json()
    local filepath = vim.fn.input("Export as JSON: ", "", "file")
    if filepath and filepath ~= "" then
      local ok, err = self.app_manager:save_result("json", filepath)
      if ok then
        local abs_path = vim.fn.fnamemodify(filepath, ":p")
        vim.notify("Exported to " .. abs_path, vim.log.levels.INFO)
      else
        vim.notify("Failed to export: " .. tostring(err), vim.log.levels.ERROR)
      end
    end
  end

  local function focus_query()
    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_query()
    end
  end

  local function focus_table_list()
    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_table_list()
    end
  end

  local commands = {
    edit_cell = edit_cell,
    apply_changes = apply_changes,
    undo_table_changes = undo_table_changes,
    undo_cell_change = undo_cell_change,
    undo_row_changes = undo_row_changes,
    export = export,
    export_csv = export_csv,
    export_json = export_json,
    focus_query = focus_query,
    focus_table_list = focus_table_list,
  }

  for action, keymap in pairs(self.bindings.result) do
    if commands[action] then
      vim.keymap.set("n", keymap, commands[action], {
        buffer = bufnr,
        desc = "neosql result: " .. action,
        silent = true,
        noremap = true,
      })
    end
  end
end

function BindingManager:register_table_list_bindings(bufnr)
  if not self.enabled or not self.bindings.table_list then
    return
  end

  local function get_table_name()
    local line = vim.api.nvim_get_current_line()
    return line:match("^%s*(%S+)%s*$")
  end

  local function insert_template()
    local table_name = get_table_name()
    if not table_name then
      return
    end

    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_query()
      local query = string.format('INSERT INTO "%s" (?) VALUES (?);', table_name)
      vim.api.nvim_buf_set_lines(
        self.app_manager.window_manager.query_buf,
        0,
        -1,
        false,
        vim.split(query, "\n")
      )
    end
  end

  local function select_template()
    local table_name = get_table_name()
    if not table_name then
      return
    end

    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_query()
      local query = string.format('SELECT ? FROM "%s" WHERE ?;', table_name)
      vim.api.nvim_buf_set_lines(
        self.app_manager.window_manager.query_buf,
        0,
        -1,
        false,
        vim.split(query, "\n")
      )
    end
  end

  local function update_template()
    local table_name = get_table_name()
    if not table_name then
      return
    end

    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_query()
      local query = string.format('UPDATE "%s" SET ? = ? WHERE ?;', table_name)
      vim.api.nvim_buf_set_lines(
        self.app_manager.window_manager.query_buf,
        0,
        -1,
        false,
        vim.split(query, "\n")
      )
    end
  end

  local function delete_template()
    local table_name = get_table_name()
    if not table_name then
      return
    end

    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_query()
      local query = string.format('DELETE FROM "%s" WHERE ?;', table_name)
      vim.api.nvim_buf_set_lines(
        self.app_manager.window_manager.query_buf,
        0,
        -1,
        false,
        vim.split(query, "\n")
      )
    end
  end

  local function select_table()
    local table_name = get_table_name()
    if not table_name then
      return
    end

    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_query()
      local query = string.format('SELECT * FROM "%s" LIMIT 100;', table_name)
      vim.api.nvim_buf_set_lines(
        self.app_manager.window_manager.query_buf,
        0,
        -1,
        false,
        vim.split(query, "\n")
      )
    end
  end

  local function focus_query()
    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_query()
    end
  end

  local function focus_result()
    if self.app_manager.window_manager then
      self.app_manager.window_manager:focus_result()
    end
  end

  local commands = {
    select_table = select_table,
    insert_template = insert_template,
    select_template = select_template,
    update_template = update_template,
    delete_template = delete_template,
    focus_query = focus_query,
    focus_result = focus_result,
  }

  for action, keymap in pairs(self.bindings.table_list) do
    if commands[action] then
      vim.keymap.set("n", keymap, commands[action], {
        buffer = bufnr,
        desc = "neosql table_list: " .. action,
        silent = true,
        noremap = true,
      })
    end
  end
end

function BindingManager:register_all_bindings(window_manager)
  if not window_manager then
    return
  end

  if window_manager.query_buf then
    self:register_query_bindings(window_manager.query_buf)
  end

  if window_manager.result_buf then
    self:register_result_bindings(window_manager.result_buf)
  end

  if window_manager.table_list_buf then
    self:register_table_list_bindings(window_manager.table_list_buf)
  end
end

function BindingManager:_get_column_index_from_line(line, columns)
  local cell_positions = {}
  local cell_start = nil
  local cell_index = 0
  local seen_first_pipe = false
  
  for i = 1, #line do
    local char = line:sub(i, i)
    if char == "|" then
      if not seen_first_pipe then
        seen_first_pipe = true
        cell_start = i + 1
      elseif cell_start then
        cell_index = cell_index + 1
        table.insert(cell_positions, {
          start = cell_start,
          end_pos = i - 1,
          index = cell_index
        })
        cell_start = i + 1
      end
    end
  end
  
  if cell_start and cell_start <= #line and line:sub(#line, #line) ~= "|" then
    cell_index = cell_index + 1
    table.insert(cell_positions, {
      start = cell_start,
      end_pos = #line,
      index = cell_index
    })
  end
  
  local cursor_col = vim.api.nvim_win_get_cursor(0)[2] + 1
  
  for _, pos in ipairs(cell_positions) do
    if cursor_col >= pos.start and cursor_col <= pos.end_pos then
      return pos.index
    end
  end

  for i, pos in ipairs(cell_positions) do
    if i < #cell_positions then
      local next_pos = cell_positions[i + 1]
      if cursor_col > pos.end_pos and cursor_col < next_pos.start then
        return pos.index
      end
    end
  end

  if #cell_positions > 0 and cursor_col < cell_positions[1].start then
    return 1
  end

  if #cell_positions > 0 and cursor_col > cell_positions[#cell_positions].end_pos then
    return cell_positions[#cell_positions].index
  end

  return 1
end

function BindingManager:_parse_value(value, original_value)
  if original_value == nil then
    if value:match("^%d+$") then
      return tonumber(value)
    elseif value:lower() == "true" then
      return true
    elseif value:lower() == "false" then
      return false
    end
    return value
  end

  local original_type = type(original_value)
  
  if original_type == "number" then
    local num = tonumber(value)
    return num or value
  elseif original_type == "boolean" then
    if value:lower() == "true" then
      return true
    elseif value:lower() == "false" then
      return false
    end
    return value
  end

  return value
end

function BindingManager:add_custom_binding(window_type, keymap, action, desc)
  if not self.enabled then
    return
  end

  if not self.bindings[window_type] then
    self.bindings[window_type] = {}
  end

  local bufnr = nil
  if window_type == "query" and self.app_manager.window_manager then
    bufnr = self.app_manager.window_manager.query_buf
  elseif window_type == "result" and self.app_manager.window_manager then
    bufnr = self.app_manager.window_manager.result_buf
  elseif window_type == "table_list" and self.app_manager.window_manager then
    bufnr = self.app_manager.window_manager.table_list_buf
  end

  if bufnr then
    vim.keymap.set("n", keymap, action, {
      buffer = bufnr,
      desc = desc or "neosql custom",
      silent = true,
      noremap = true,
    })
  end
end

function BindingManager:get_bindings()
  return vim.deepcopy(self.bindings)
end

return BindingManager
