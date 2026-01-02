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

function Postgres:get_primary_keys(table_name, schema)
  schema = schema or 'public'
  
  if not table_name or table_name == "" then
    return {}
  end
  
  local clean_table_name = table_name:gsub('^"', ''):gsub('"$', '')
  local escaped_table_name = clean_table_name:gsub("'", "''")
  local escaped_schema = schema:gsub("'", "''")
  
  local sql = string.format([[
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

  local ok, result = pcall(function()
    return self:query(sql)
  end)

  if not ok or not result then
    sql = string.format([[
      SELECT column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_name = '%s'
        AND tc.table_schema = '%s'
      ORDER BY kcu.ordinal_position;
    ]], escaped_table_name, escaped_schema)

    ok, result = pcall(function()
      return self:query(sql)
    end)

    if not ok or not result then
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

function Postgres:get_table_columns(table_name, schema)
  schema = schema or 'public'
  if not table_name or table_name == "" then
    return nil, "Table name is required"
  end

  local clean_table_name = table_name:gsub('^"', ''):gsub('"$', '')
  local escaped_table_name = clean_table_name:gsub("'", "''")
  local escaped_schema = schema:gsub("'", "''")

  local sql = string.format([[
    SELECT 
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale,
      is_nullable,
      column_default,
      ordinal_position
    FROM information_schema.columns
    WHERE table_schema = '%s'
      AND table_name = '%s'
    ORDER BY ordinal_position;
  ]], escaped_schema, escaped_table_name)

  local ok, result = pcall(function()
    return self:query(sql)
  end)

  if not ok then
    return nil, result
  end

  if not result or not result.rows then
    return nil, "No columns found"
  end

  local rows = result.rows
  local columns = result.columns

  if not rows or #rows == 0 then
    return nil, "No columns found"
  end

  return {
    rows = rows,
    columns = columns,
  }, nil
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

    local sql = string.format('UPDATE %s SET %s WHERE %s',
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

function Postgres:apply_inserts(table_name, inserts)
  if not table_name or table_name == "" then
    return false, "Table name is required"
  end

  if not inserts or #inserts == 0 then
    return false, "No inserts to apply"
  end

  for _, insert in ipairs(inserts) do
    if not insert.values then
      return false, "Invalid insert format: missing values"
    end

    local columns = {}
    local values = {}
    for col, val in pairs(insert.values) do
      table.insert(columns, '"' .. col .. '"')
      if val == nil then
        table.insert(values, "NULL")
      elseif type(val) == "string" then
        val = string.gsub(val, "'", "''")
        table.insert(values, "'" .. val .. "'")
      elseif type(val) == "number" then
        table.insert(values, tostring(val))
      elseif type(val) == "boolean" then
        table.insert(values, val and "TRUE" or "FALSE")
      else
        table.insert(values, "'" .. tostring(val) .. "'")
      end
    end

    if #columns == 0 then
      return false, "No columns to insert"
    end

    local sql = string.format('INSERT INTO %s (%s) VALUES (%s)',
      table_name,
      table.concat(columns, ", "),
      table.concat(values, ", "))

    local ok, result = pcall(function()
      return self:query(sql)
    end)

    if not ok then
      return false, "Failed to apply insert: " .. tostring(result)
    end

    if result and result.message then
      local msg = result.message
      if msg:match("ERROR") or msg:match("error:") then
        return false, "Insert failed: " .. msg
      end
    end
  end

  return true, nil
end

function Postgres:apply_deletes(table_name, deletes)
  if not table_name or table_name == "" then
    return false, "Table name is required"
  end

  if not deletes or #deletes == 0 then
    return false, "No deletes to apply"
  end

  for _, delete in ipairs(deletes) do
    if not delete.primary_key then
      return false, "Invalid delete format: missing primary_key"
    end

    local where_parts = {}
    for pk, pk_value in pairs(delete.primary_key) do
      if type(pk_value) == "string" then
        pk_value = string.gsub(pk_value, "'", "''")
        table.insert(where_parts, '"' .. pk .. '" = \'' .. pk_value .. '\'')
      else
        table.insert(where_parts, '"' .. pk .. '" = ' .. tostring(pk_value))
      end
    end

    local sql = string.format('DELETE FROM %s WHERE %s',
      table_name,
      table.concat(where_parts, " AND "))

    local ok, result = pcall(function()
      return self:query(sql)
    end)

    if not ok then
      return false, "Failed to apply delete: " .. tostring(result)
    end

    if result and result.message then
      local msg = result.message
      if msg:match("ERROR") or msg:match("error:") then
        return false, "Delete failed: " .. msg
      end
    end
  end

  return true, nil
end

function Postgres:_normalize_column_name(column_name)
  if not column_name or column_name == "" then
    return nil, nil
  end
  local clean = column_name:gsub('^"', ''):gsub('"$', '')
  return '"' .. clean .. '"', clean
end

function Postgres:apply_table_properties(table_name, updates)
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

    local column_name_raw = update.primary_key.column_name
    if not column_name_raw then
      return false, "Column name is required for table property update"
    end

    local column_name, _ = self:_normalize_column_name(column_name_raw)
    if not column_name then
      return false, "Invalid column name"
    end

    if not update.original_row then
      local _, clean_name = self:_normalize_column_name(column_name_raw)
      return false, "Original row data is required for table property update (column: " .. tostring(clean_name) .. ")"
    end

    local original_row = update.original_row
    local alter_statements = {}

    local column_rename_stmt = nil
    local new_column_name = column_name
    
    for field, new_value in pairs(update.changes) do
      if field == "column_name" then
        local new_col_name_quoted, new_col_name_clean = self:_normalize_column_name(tostring(new_value))
        local _, current_col_name_clean = self:_normalize_column_name(column_name_raw)
        if new_col_name_quoted and new_col_name_clean ~= "" and new_col_name_clean ~= current_col_name_clean then
          new_column_name = new_col_name_quoted
          column_rename_stmt = string.format('RENAME COLUMN %s TO %s', column_name, new_column_name)
        end
      end
    end
    
    if column_rename_stmt then
      column_name = new_column_name
    end
    
    for field, new_value in pairs(update.changes) do
      local original_value = original_row[field]
      
      if field == "column_name" then
      elseif field == "is_nullable" then
        local original_str = original_value and tostring(original_value):upper():gsub("^%s+", ""):gsub("%s+$", "") or ""
        local new_str = new_value and tostring(new_value):upper():gsub("^%s+", ""):gsub("%s+$", "") or ""
        
        if original_str ~= new_str then
          if new_str == "NO" then
            table.insert(alter_statements, string.format('ALTER COLUMN %s SET NOT NULL', column_name))
          elseif new_str == "YES" then
            table.insert(alter_statements, string.format('ALTER COLUMN %s DROP NOT NULL', column_name))
          end
        end
      elseif field == "column_default" then
        local original_default_str = original_value and tostring(original_value) or ""
        local new_default_str = new_value and tostring(new_value) or ""
        
        local original_is_empty = original_value == nil or original_value == ""
        local new_is_empty = new_value == nil or new_value == ""
        
        if original_is_empty and new_is_empty then
        elseif original_is_empty ~= new_is_empty or original_default_str ~= new_default_str then
          if new_is_empty then
            table.insert(alter_statements, string.format('ALTER COLUMN %s DROP DEFAULT', column_name))
          else
            local default_value = tostring(new_value)
            if type(new_value) == "string" then
              default_value = string.gsub(default_value, "'", "''")
              if not (default_value:match("^%(") or default_value:match("^'") or default_value:match("^%-?%d") or default_value:upper() == "TRUE" or default_value:upper() == "FALSE" or default_value:upper() == "NULL") then
                default_value = "'" .. default_value .. "'"
              end
            end
            table.insert(alter_statements, string.format('ALTER COLUMN %s SET DEFAULT %s', column_name, default_value))
          end
        end
      end
    end

    if column_rename_stmt then
      local sql = string.format('ALTER TABLE %s %s',
        table_name,
        column_rename_stmt)

      local ok, result = pcall(function()
        return self:query(sql)
      end)

      if not ok then
        return false, "Failed to rename column: " .. tostring(result) .. " (SQL: " .. sql .. ")"
      end

      if result and result.message then
        local msg = result.message
        if msg:match("ERROR") or msg:match("error:") then
          return false, "Column rename failed: " .. msg .. " (SQL: " .. sql .. ")"
        end
      end
    end

    if #alter_statements == 0 then
      if not column_rename_stmt then
        goto continue
      else
        return true, nil
      end
    end

    for _, alter_stmt in ipairs(alter_statements) do
      local sql = string.format('ALTER TABLE %s %s',
        table_name,
        alter_stmt)

      local ok, result = pcall(function()
        return self:query(sql)
      end)

      if not ok then
        return false, "Failed to apply table property change: " .. tostring(result) .. " (SQL: " .. sql .. ")"
      end

      if result and result.message then
        local msg = result.message
        if msg:match("ERROR") or msg:match("error:") then
          return false, "Table property change failed: " .. msg .. " (SQL: " .. sql .. ")"
        end
      end
    end

    ::continue::
  end

  return true, nil
end

function Postgres:apply_table_property_inserts(table_name, inserts)
  if not table_name or table_name == "" then
    return false, "Table name is required"
  end

  if not inserts or #inserts == 0 then
    return false, "No inserts to apply"
  end

  for _, insert in ipairs(inserts) do
    if not insert.values then
      return false, "Invalid insert format: missing values"
    end

    local column_name_raw = insert.values.column_name
    if not column_name_raw or column_name_raw == "" then
      return false, "Column name is required for new column"
    end

    local column_name, _ = self:_normalize_column_name(column_name_raw)
    if not column_name then
      return false, "Invalid column name"
    end

    local data_type = insert.values.data_type
    if not data_type or data_type == "" then
      return false, "Data type is required for new column"
    end

    local column_definition_parts = { column_name, data_type }

    local is_nullable = insert.values.is_nullable
    if is_nullable and (is_nullable == "NO" or is_nullable:upper() == "NO") then
      table.insert(column_definition_parts, "NOT NULL")
    end

    local column_default = insert.values.column_default
    if column_default and column_default ~= "" then
      local default_value = tostring(column_default)
      if type(column_default) == "string" then
        default_value = string.gsub(default_value, "'", "''")
        if not (default_value:match("^%(") or default_value:match("^'") or default_value:match("^%-?%d") or default_value:upper() == "TRUE" or default_value:upper() == "FALSE" or default_value:upper() == "NULL") then
          default_value = "'" .. default_value .. "'"
        end
      end
      table.insert(column_definition_parts, "DEFAULT " .. default_value)
    end

    local sql = string.format('ALTER TABLE %s ADD COLUMN %s',
      table_name,
      table.concat(column_definition_parts, " "))

    local ok, result = pcall(function()
      return self:query(sql)
    end)

    if not ok then
      return false, "Failed to add column: " .. tostring(result) .. " (SQL: " .. sql .. ")"
    end

    if result and result.message then
      local msg = result.message
      if msg:match("ERROR") or msg:match("error:") then
        return false, "Column add failed: " .. msg .. " (SQL: " .. sql .. ")"
      end
    end
  end

  return true, nil
end

function Postgres:apply_table_property_deletes(table_name, deletes)
  if not table_name or table_name == "" then
    return false, "Table name is required"
  end

  if not deletes or #deletes == 0 then
    return false, "No deletes to apply"
  end

  for _, delete in ipairs(deletes) do
    if not delete.primary_key or not delete.primary_key.column_name then
      return false, "Invalid delete format: missing column name"
    end

    local column_name_raw = delete.primary_key.column_name
    local column_name, _ = self:_normalize_column_name(column_name_raw)
    if not column_name then
      return false, "Invalid column name"
    end

    local sql = string.format('ALTER TABLE %s DROP COLUMN %s',
      table_name,
      column_name)

    local ok, result = pcall(function()
      return self:query(sql)
    end)

    if not ok then
      return false, "Failed to delete column: " .. tostring(result) .. " (SQL: " .. sql .. ")"
    end

    if result and result.message then
      local msg = result.message
      if msg:match("ERROR") or msg:match("error:") then
        return false, "Column delete failed: " .. msg .. " (SQL: " .. sql .. ")"
      end
    end
  end

  return true, nil
end

return Postgres
