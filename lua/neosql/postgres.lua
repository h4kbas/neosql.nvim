local Postgres = {}
Postgres.__index = Postgres

function Postgres.new(config)
  local self = setmetatable({}, Postgres)
  
  if type(config) == "string" then
    self.connection_string = config
  else
    if not config.database then
      error("database is required")
    end
    local parts = {}
    if config.user then
      table.insert(parts, config.user)
      if config.password then
        table.insert(parts, ":" .. config.password)
      end
      table.insert(parts, "@")
    end
    table.insert(parts, config.host or "localhost")
    if config.port and config.port ~= 5432 then
      table.insert(parts, ":" .. config.port)
    end
    table.insert(parts, "/" .. config.database)
    self.connection_string = "postgresql://" .. table.concat(parts)
  end
  
  self.connection = nil
  return self
end

function Postgres:connect()
  if self.connection then
    return true
  end

  local has_plenary, Job = pcall(require, "plenary.job")
  local use_plenary = has_plenary and Job ~= nil

  local cmd = "psql"
  local args = { self.connection_string, "-A", "-F", "|", "-c", "SELECT 1;" }

  local ok = false
  local error_msg = nil
  
  if use_plenary then
    local error_output = {}
    local stdout_output = {}
    local job = Job:new({
      command = cmd,
      args = args,
      on_stdout = function(_, data)
        table.insert(stdout_output, data)
      end,
      on_stderr = function(_, data)
        table.insert(error_output, data)
      end,
    })
    local exit_code = job:sync()
    
    local has_success_output = false
    local combined_output = table.concat(stdout_output, "\n") .. "\n" .. table.concat(error_output, "\n")
    if combined_output:match("%(1 row%)") or combined_output:match("^1%s*$") or combined_output:match("^%s*1%s*$") then
      has_success_output = true
    end
    
    ok = exit_code == 0 or has_success_output
    
    if not ok then
      local all_output = {}
      if #error_output > 0 then
        for _, line in ipairs(error_output) do
          table.insert(all_output, line)
        end
      end
      if #stdout_output > 0 then
        for _, line in ipairs(stdout_output) do
          table.insert(all_output, line)
        end
      end
      error_msg = table.concat(all_output, "\n")
    end
  else
    local escaped_conn = "'" .. self.connection_string:gsub("'", "'\"'\"'") .. "'"
    local full_cmd = cmd .. " " .. escaped_conn .. " -A -F \"|\" -c \"SELECT 1;\" 2>&1"
    local handle = io.popen(full_cmd, "r")
    if handle then
      local output = handle:read("*a")
      handle:close()
      
      if output:match("psql: error") or output:match("psql: ERROR") or 
         output:match("could not connect") or 
         output:match("connection to server") or
         output:match("authentication failed") or
         output:match("password authentication failed") then
        ok = false
        error_msg = output
      elseif output:match("%(1 row%)") or output:match("^%s*1%s*$") or output:match("\n1\n") or output:match("\n1%s*\n") then
        ok = true
      else
        if output:match("?column?") and output:match("%d+") then
          ok = true
        else
          ok = false
          error_msg = output
        end
      end
    else
      ok = false
      error_msg = "Failed to execute psql command"
    end
  end

  if not ok then
    local msg = "Failed to connect to PostgreSQL."
    if error_msg and error_msg ~= "" then
      msg = msg .. " Error: " .. vim.trim(error_msg)
    else
      msg = msg .. " Make sure psql is installed and connection string is valid."
    end
    error(msg)
  end

  self.connection = true
  return true
end

function Postgres:disconnect()
  self.connection = nil
end

function Postgres:query(sql)
  if not self.connection then
    error("Not connected to database")
  end

  local has_plenary, Job = pcall(require, "plenary.job")
  local use_plenary = has_plenary and Job ~= nil

  local cmd = "psql"
  local args = { self.connection_string, "--no-password", "-A", "-F", "|", "-c", sql }

  local output = {}
  local error_output = {}

  if use_plenary then
    local job = Job:new({
      command = cmd,
      args = args,
      on_stdout = function(_, data)
        for line in data:gmatch("[^\r\n]+") do
          table.insert(output, line)
        end
      end,
      on_stderr = function(_, data)
        table.insert(error_output, data)
      end,
    })

    job:sync()

    if #error_output > 0 then
      local error_msg = table.concat(error_output, "\n")
      error("Query failed: " .. error_msg)
    end
  else
    local handle = io.popen(cmd .. " " .. table.concat(args, " ") .. " 2>&1", "r")
    if not handle then
      error("Failed to execute query")
    end

    for line in handle:lines() do
      table.insert(output, line)
    end
    handle:close()

    for _, line in ipairs(output) do
      if line:match("ERROR") or line:match("error:") then
        error("Query failed: " .. line)
      end
    end
  end

  return self:_parse_query_result(output, sql)
end

function Postgres:_parse_query_result(output, query)
  if #output == 0 then
    return { rows = {}, columns = {} }
  end

  local lines = output
  if type(output) == "string" then
    lines = vim.split(output, "\n")
  end

  local query_upper = string.upper(vim.trim(query))
  if query_upper:match("^SELECT") then
    return self:_parse_select_result(lines)
  else
    return {
      rows = {},
      columns = {},
      affected_rows = 0,
      message = table.concat(lines, "\n"),
    }
  end
end

function Postgres:_parse_select_result(lines)
  if #lines == 0 then
    return { rows = {}, columns = {} }
  end

  local clean_lines = self:_clean_output_lines(lines)
  if #clean_lines == 0 then
    return { rows = {}, columns = {} }
  end

  local header_line = nil
  local header_idx = 1
  for i = 1, #clean_lines do
    local line = clean_lines[i]
    if line and not self:_is_separator_line(line) then
      header_line = line
      header_idx = i
      break
    end
  end

  if not header_line then
    return { rows = {}, columns = {} }
  end

  local columns = self:_extract_columns(header_line)
  if #columns == 0 then
    return { rows = {}, columns = {} }
  end

  local rows = self:_extract_rows(clean_lines, columns, header_idx)
  return { rows = rows, columns = columns }
end

function Postgres:_clean_output_lines(lines)
  local clean_lines = {}
  for _, line in ipairs(lines) do
    if line and line:match("%S") 
       and not line:match("^%s*$")
       and not line:match("^%s*%-+%s*$")
       and not line:match("^%s*%(%d+ rows?%)%s*$")
       and not line:match("^%s*%-+%s*%-+%s*$") then
      table.insert(clean_lines, line)
    end
  end
  return clean_lines
end

function Postgres:_extract_columns(header_line)
  local columns = {}
  
  if not header_line or header_line == "" then
    return columns
  end

  local parts = {}
  local current = ""
  
  for i = 1, #header_line do
    local char = header_line:sub(i, i)
    if char == "|" then
      table.insert(parts, current)
      current = ""
    else
      current = current .. char
    end
  end
  
  table.insert(parts, current)
  
  for _, part in ipairs(parts) do
    local col = vim.trim(part)
    table.insert(columns, col)
  end

  return columns
end

function Postgres:_is_separator_line(line)
  if not line then
    return true
  end
  local cleaned = line:gsub("[|%s%-%+]", "")
  return cleaned == ""
end

function Postgres:_extract_rows(clean_lines, columns, start_idx)
  local rows = {}
  start_idx = start_idx or 1
  
  for i = start_idx + 1, #clean_lines do
    local line = clean_lines[i]
    
    if line and not self:_is_separator_line(line) then
      local cells = {}
      
      local parts = {}
      local current = ""
      for j = 1, #line do
        local char = line:sub(j, j)
        if char == "|" then
          table.insert(parts, current)
          current = ""
        else
          current = current .. char
        end
      end
      table.insert(parts, current)
      
      local num_parts = #parts
      for idx = 1, num_parts do
        local part = parts[idx]
        local cell_value = self:_parse_cell_value(part)
        cells[idx] = cell_value
      end
      
      for idx = num_parts + 1, #columns do
        cells[idx] = nil
      end
      
      local row_obj = self:_build_row_object(cells, columns)
      if row_obj and next(row_obj) ~= nil then
        table.insert(rows, row_obj)
      end
    end
  end

  return rows
end

function Postgres:_extract_cells(line)
  local cells = {}
  
  if not line then
    return cells
  end

  local parts = vim.split(line, "|", { plain = true })
  
  for _, part in ipairs(parts) do
    local cell_value = self:_parse_cell_value(part)
    table.insert(cells, cell_value)
  end

  return cells
end

function Postgres:_parse_cell_value(part)
  if not part then
    return nil
  end
  
  if part == "\\N" or part:match("^%s*\\N%s*$") then
    return nil
  end
  
  local trimmed = vim.trim(part)
  
  if trimmed == "" then
    return nil
  end
  
  if trimmed:lower() == "null" then
    return nil
  end
  
  return trimmed
end

function Postgres:_build_row_object(cells, columns)
  if not columns or #columns == 0 then
    return {}
  end

  cells = cells or {}
  
  local row_obj = {}
  local num_cells = #columns
  for j = 1, #columns do
    if j <= num_cells then
      row_obj[columns[j]] = cells[j]
    else
      row_obj[columns[j]] = nil
    end
  end

  return row_obj
end

function Postgres:is_connected()
  return self.connection ~= nil
end

function Postgres:get_tables(schema)
  schema = schema or 'public'
  local sql = string.format([[
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '%s' 
    ORDER BY table_name;
  ]], schema)

  local ok, result = pcall(function()
    return self:query(sql)
  end)

  if not ok then
    return nil, result
  end

  local tables = {}
  local rows = result
  if result and result.rows then
    rows = result.rows
  end
  
  if rows and type(rows) == "table" then
    if #rows > 0 then
      for _, row in ipairs(rows) do
        if row and type(row) == "table" then
          local table_name = row.table_name or row["table_name"]
          if table_name and table_name ~= "" then
            table.insert(tables, tostring(table_name))
          end
        end
      end
    end
  end

  return tables, nil
end

function Postgres:get_primary_keys(table_name, schema, was_quoted)
  schema = schema or 'public'
  
  if not table_name or table_name == "" then
    return {}
  end
  
  local clean_table_name = table_name:gsub('^"', ''):gsub('"$', '')
  local escaped_table_name = clean_table_name:gsub("'", "''")
  local escaped_schema = schema:gsub("'", "''")
  
  local sql
  if was_quoted then
    sql = string.format([[
      SELECT a.attname AS column_name
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE i.indisprimary = true
        AND c.relname = '%s'
        AND n.nspname = '%s'
      ORDER BY array_position(i.indkey, a.attnum);
    ]], escaped_table_name, escaped_schema)
  else
    sql = string.format([[
      SELECT a.attname AS column_name
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE i.indisprimary = true
        AND c.relname = LOWER('%s')
        AND n.nspname = '%s'
      ORDER BY array_position(i.indkey, a.attnum);
    ]], escaped_table_name, escaped_schema)
  end

  local ok, result = pcall(function()
    return self:query(sql)
  end)

  if not ok or not result then
    if not was_quoted then
      sql = string.format([[
        SELECT column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND LOWER(tc.table_name) = LOWER('%s')
          AND tc.table_schema = '%s'
        ORDER BY kcu.ordinal_position;
      ]], escaped_table_name, escaped_schema)

      ok, result = pcall(function()
        return self:query(sql)
      end)

      if not ok or not result then
        return {}
      end
    else
      return {}
    end
  end

  local rows = result
  if result and result.rows then
    rows = result.rows
  end

  local primary_keys = {}
  if rows and type(rows) == "table" then
    for _, row in ipairs(rows) do
      if row and row.column_name then
        table.insert(primary_keys, row.column_name)
      end
    end
  end

  return primary_keys
end

function Postgres:extract_table_name(query)
  if not query then
    return nil, false
  end

  local normalized = query:gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")
  local lower_normalized = string.lower(normalized)

  local keywords = { 'from', 'update', 'into' }

  for _, keyword in ipairs(keywords) do
    local keyword_pos = lower_normalized:find(keyword, 1, true)
    if keyword_pos then
      local after_keyword = normalized:sub(keyword_pos + #keyword)
      local quoted_match = after_keyword:match('^%s+"([^"]+)"')
      if quoted_match then
        return quoted_match, true
      end
    end
  end

  local unquoted_patterns = {
    "from%s+([%w_]+)",
    "update%s+([%w_]+)",
    "into%s+([%w_]+)",
  }

  for _, pattern in ipairs(unquoted_patterns) do
    local table_name = lower_normalized:match(pattern)
    if table_name then
      return table_name, false
    end
  end

  return nil, false
end

function Postgres:apply_updates(table_name, updates)
  if not table_name or table_name == "" then
    return false, "Table name is required"
  end

  if not updates or #updates == 0 then
    return false, "No updates to apply"
  end

  for _, update in ipairs(updates) do
    if not update.primary_key or not update.changes then
      return false, "Invalid update format: missing primary_key or changes"
    end

    local set_parts = {}
    for field, value in pairs(update.changes) do
      if value == nil then
        table.insert(set_parts, '"' .. field .. '" = NULL')
      elseif type(value) == "string" then
        value = string.gsub(value, "'", "''")
        table.insert(set_parts, '"' .. field .. '" = \'' .. value .. '\'')
      elseif type(value) == "number" then
        table.insert(set_parts, '"' .. field .. '" = ' .. tostring(value))
      elseif type(value) == "boolean" then
        table.insert(set_parts, '"' .. field .. '" = ' .. (value and "TRUE" or "FALSE"))
      else
        table.insert(set_parts, '"' .. field .. '" = \'' .. tostring(value) .. '\'')
      end
    end

    local where_parts = {}
    for pk, pk_value in pairs(update.primary_key) do
      if type(pk_value) == "string" then
        pk_value = string.gsub(pk_value, "'", "''")
        table.insert(where_parts, '"' .. pk .. '" = \'' .. pk_value .. '\'')
      else
        table.insert(where_parts, '"' .. pk .. '" = ' .. tostring(pk_value))
      end
    end

    local sql = string.format('UPDATE "%s" SET %s WHERE %s',
      table_name,
      table.concat(set_parts, ", "),
      table.concat(where_parts, " AND "))

    local ok, result = pcall(function()
      return self:query(sql)
    end)

    if not ok then
      return false, "Failed to apply update: " .. tostring(result)
    end

    if result and result.message then
      local msg = result.message
      if msg:match("ERROR") or msg:match("error:") then
        return false, "Update failed: " .. msg
      end
    end
  end

  return true, nil
end

return Postgres
