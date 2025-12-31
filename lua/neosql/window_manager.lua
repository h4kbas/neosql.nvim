local WindowManager = {}
WindowManager.__index = WindowManager

function WindowManager.new()
  local self = setmetatable({}, WindowManager)
  self.table_list_buf = nil
  self.query_buf = nil
  self.result_buf = nil
  self.table_list_win = nil
  self.query_win = nil
  self.result_win = nil
  self.null_namespace = nil
  self.boolean_namespace = nil
  self.edited_namespace = nil
  self.current_data_manager = nil
  return self
end

function WindowManager:create()
  local current_win = vim.api.nvim_get_current_win()
  local current_buf = vim.api.nvim_get_current_buf()

  vim.cmd('topleft vsplit')
  self.table_list_win = vim.api.nvim_get_current_win()
  self.table_list_buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_win_set_buf(self.table_list_win, self.table_list_buf)
  vim.api.nvim_buf_set_name(self.table_list_buf, 'neosql://table_list')
  vim.api.nvim_buf_set_option(self.table_list_buf, 'filetype', 'neosql-table-list')
  vim.api.nvim_buf_set_option(self.table_list_buf, 'modifiable', false)
  vim.api.nvim_win_set_option(self.table_list_win, 'number', false)
  vim.api.nvim_win_set_option(self.table_list_win, 'relativenumber', false)

  vim.api.nvim_set_current_win(current_win)
  vim.cmd('split')
  self.query_win = vim.api.nvim_get_current_win()
  self.query_buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_win_set_buf(self.query_win, self.query_buf)
  vim.api.nvim_buf_set_name(self.query_buf, 'neosql://query')
  vim.api.nvim_buf_set_option(self.query_buf, 'filetype', 'sql')

  vim.cmd('split')
  self.result_win = vim.api.nvim_get_current_win()
  self.result_buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_win_set_buf(self.result_win, self.result_buf)
  vim.api.nvim_buf_set_name(self.result_buf, 'neosql://result')
  vim.api.nvim_buf_set_option(self.result_buf, 'filetype', 'markdown')
  vim.api.nvim_buf_set_option(self.result_buf, 'modifiable', false)
  vim.api.nvim_win_set_option(self.result_win, 'wrap', false)

  self:resize_windows()
  vim.api.nvim_set_current_win(self.query_win)
end

function WindowManager:resize_windows()
  local width = vim.api.nvim_win_get_width(0)
  local height = vim.api.nvim_win_get_height(0)

  if self.table_list_win and vim.api.nvim_win_is_valid(self.table_list_win) then
    vim.api.nvim_win_set_width(self.table_list_win, math.floor(width * 0.3))
  end

  if self.query_win and vim.api.nvim_win_is_valid(self.query_win) then
    vim.api.nvim_win_set_height(self.query_win, math.floor(height * 0.4))
  end

  if self.result_win and vim.api.nvim_win_is_valid(self.result_win) then
    vim.api.nvim_win_set_height(self.result_win, math.floor(height * 0.6))
  end
end

function WindowManager:set_table_list(content)
  if not self.table_list_buf then
    return
  end
  vim.api.nvim_buf_set_option(self.table_list_buf, 'modifiable', true)
  vim.api.nvim_buf_set_lines(self.table_list_buf, 0, -1, false, content)
  vim.api.nvim_buf_set_option(self.table_list_buf, 'modifiable', false)
end

function WindowManager:get_query()
  if not self.query_buf then
    return nil
  end
  return table.concat(vim.api.nvim_buf_get_lines(self.query_buf, 0, -1, false), '\n')
end

function WindowManager:set_result(content, data_manager)
  if not self.result_buf then
    return
  end
  
  local lines = {}
  if type(content) == "string" then
    lines = vim.split(content, "\n")
  else
    for _, line in ipairs(content) do
      if type(line) == "string" then
        local split_lines = vim.split(line, "\n")
        for _, split_line in ipairs(split_lines) do
          table.insert(lines, split_line)
        end
      else
        table.insert(lines, tostring(line))
      end
    end
  end
  
  vim.api.nvim_buf_set_option(self.result_buf, 'modifiable', true)
  vim.api.nvim_buf_set_lines(self.result_buf, 0, -1, false, lines)
  vim.api.nvim_buf_set_option(self.result_buf, 'modifiable', false)
  
  self.current_data_manager = data_manager
  
  self:_highlight_null_values()
  self:_highlight_boolean_values()
  self:_highlight_edited_cells()
end

function WindowManager:_highlight_null_values()
  if not self.result_buf then
    return
  end
  
  local hl_group = "NeoSqlNull"
  vim.cmd(string.format("highlight %s guifg=#808080 gui=italic", hl_group))
  
  if not self.null_namespace then
    self.null_namespace = vim.api.nvim_create_namespace("neosql_null")
  end
  
  vim.api.nvim_buf_clear_namespace(self.result_buf, self.null_namespace, 0, -1)
  
  local lines = vim.api.nvim_buf_get_lines(self.result_buf, 0, -1, false)
  for line_num, line in ipairs(lines) do
    if line:match("^%s*|%s*%-+") then
      goto continue
    end
    
    if not line:match("|") then
      goto continue
    end
    
    local start_col = 1
    while true do
      local null_start, null_end = line:find("NULL", start_col, true)
      if not null_start then
        break
      end
      
      local before_char = null_start > 1 and line:sub(null_start - 1, null_start - 1) or " "
      local after_char = null_end < #line and line:sub(null_end + 1, null_end + 1) or " "
      
      if (before_char == " " or before_char == "|") and (after_char == " " or after_char == "|") then
        vim.api.nvim_buf_add_highlight(
          self.result_buf,
          self.null_namespace,
          hl_group,
          line_num - 1,
          null_start - 1,
          null_end
        )
      end
      
      start_col = null_end + 1
    end
    
    ::continue::
  end
end

function WindowManager:_highlight_boolean_values()
  if not self.result_buf then
    return
  end
  
  local hl_group_true = "NeoSqlTrue"
  local hl_group_false = "NeoSqlFalse"
  vim.cmd(string.format("highlight %s guifg=#4ec9b0", hl_group_true))
  vim.cmd(string.format("highlight %s guifg=#f48771", hl_group_false))
  
  if not self.boolean_namespace then
    self.boolean_namespace = vim.api.nvim_create_namespace("neosql_boolean")
  end
  
  vim.api.nvim_buf_clear_namespace(self.result_buf, self.boolean_namespace, 0, -1)
  
  local lines = vim.api.nvim_buf_get_lines(self.result_buf, 0, -1, false)
  for line_num, line in ipairs(lines) do
    if line:match("^%s*|%s*%-+") then
      goto continue
    end
    
    if not line:match("|") then
      goto continue
    end
    
    local start_col = 1
    while true do
      local t_start, t_end = line:find(" t ", start_col)
      local f_start, f_end = line:find(" f ", start_col)
      local pipe_t_start = line:find("|t ", start_col)
      local pipe_f_start = line:find("|f ", start_col)
      
      local found_start = nil
      local found_char = nil
      local next_start = nil
      
      if t_start and (not f_start or t_start < f_start) and (not pipe_t_start or t_start < pipe_t_start) and (not pipe_f_start or t_start < pipe_f_start) then
        found_start = t_start + 1
        found_char = "t"
        next_start = t_end + 1
      elseif f_start and (not pipe_t_start or f_start < pipe_t_start) and (not pipe_f_start or f_start < pipe_f_start) then
        found_start = f_start + 1
        found_char = "f"
        next_start = f_end + 1
      elseif pipe_t_start and (not pipe_f_start or pipe_t_start < pipe_f_start) then
        found_start = pipe_t_start + 1
        found_char = "t"
        next_start = pipe_t_start + 3
      elseif pipe_f_start then
        found_start = pipe_f_start + 1
        found_char = "f"
        next_start = pipe_f_start + 3
      else
        break
      end
      
      if found_start then
        local hl_group = found_char == "t" and hl_group_true or hl_group_false
        vim.api.nvim_buf_add_highlight(
          self.result_buf,
          self.boolean_namespace,
          hl_group,
          line_num - 1,
          found_start - 1,
          found_start
        )
        start_col = next_start
      else
        break
      end
    end
    
    ::continue::
  end
end

function WindowManager:_highlight_edited_cells()
  if not self.result_buf then
    return
  end
  
  local hl_group = "NeoSqlEdited"
  vim.cmd(string.format("highlight %s guifg=#4fc3f7 gui=bold", hl_group))
  
  if not self.edited_namespace then
    self.edited_namespace = vim.api.nvim_create_namespace("neosql_edited")
  end
  
  vim.api.nvim_buf_clear_namespace(self.result_buf, self.edited_namespace, 0, -1)
  
  local changed_cells = self.current_data_manager:get_change_positions()
  local columns = self.current_data_manager:get_columns()
  
  local lines = vim.api.nvim_buf_get_lines(self.result_buf, 0, -1, false)
  local header_line_idx = nil
  
  for i, line in ipairs(lines) do
    if line:match("|") and not line:match("^%s*|%s*%-+") then
      header_line_idx = i
      break
    end
  end
  
  if not header_line_idx then
    return
  end
  
  local header_line = lines[header_line_idx]
  local column_names = {}
  local current = ""
  for i = 1, #header_line do
    local char = header_line:sub(i, i)
    if char == "|" then
      if current ~= "" then
        local col_name = vim.trim(current)
        if col_name ~= "" then
          table.insert(column_names, col_name)
        end
      end
      current = ""
    else
      current = current .. char
    end
  end
  
  local column_name_to_index = {}
  for idx, col_name in ipairs(column_names) do
    column_name_to_index[col_name] = idx
  end
  
  for line_num = header_line_idx + 2, #lines do
    local line = lines[line_num]
    if not line or not line:match("|") or line:match("^%s*|%s*%-+") then
      goto continue
    end
    
    local row_index = line_num - header_line_idx - 1
    
    if changed_cells[row_index] then
      local cell_starts = {}
      local cell_start = 1
      
      for i = 1, #line do
        local char = line:sub(i, i)
        if char == "|" then
          if i == 1 then
            cell_start = 2
          else
            table.insert(cell_starts, cell_start)
            cell_start = i + 1
          end
        end
      end
      
      table.insert(cell_starts, cell_start)
      
      for col_name, _ in pairs(changed_cells[row_index]) do
        local col_idx = column_name_to_index[col_name]
        if col_idx and cell_starts[col_idx] then
          local cell_start_pos = cell_starts[col_idx]
          local cell_end_pos = cell_starts[col_idx + 1] and (cell_starts[col_idx + 1] - 2) or (#line - 1)
          
          local value_start = cell_start_pos
          local value_end = cell_end_pos
          
          for i = cell_start_pos, cell_end_pos do
            local char = line:sub(i, i)
            if char ~= " " then
              value_start = i
              break
            end
          end
          
          for i = cell_end_pos, cell_start_pos, -1 do
            local char = line:sub(i, i)
            if char ~= " " then
              value_end = i
              break
            end
          end
          
          if value_start <= value_end then
            vim.api.nvim_buf_add_highlight(
              self.result_buf,
              self.edited_namespace,
              hl_group,
              line_num - 1,
              value_start - 1,
              value_end
            )
          end
        end
      end
    end
    
    ::continue::
  end
end

function WindowManager:focus_query()
  if self.query_win and vim.api.nvim_win_is_valid(self.query_win) then
    vim.api.nvim_set_current_win(self.query_win)
  end
end

function WindowManager:focus_table_list()
  if self.table_list_win and vim.api.nvim_win_is_valid(self.table_list_win) then
    vim.api.nvim_set_current_win(self.table_list_win)
  end
end

function WindowManager:focus_result()
  if self.result_win and vim.api.nvim_win_is_valid(self.result_win) then
    vim.api.nvim_set_current_win(self.result_win)
  end
end

function WindowManager:close()
  if self.table_list_win and vim.api.nvim_win_is_valid(self.table_list_win) then
    vim.api.nvim_win_close(self.table_list_win, false)
  end
  if self.query_win and vim.api.nvim_win_is_valid(self.query_win) then
    vim.api.nvim_win_close(self.query_win, false)
  end
  if self.result_win and vim.api.nvim_win_is_valid(self.result_win) then
    vim.api.nvim_win_close(self.result_win, false)
  end
  self.table_list_buf = nil
  self.query_buf = nil
  self.result_buf = nil
  self.table_list_win = nil
  self.query_win = nil
  self.result_win = nil
end

return WindowManager

