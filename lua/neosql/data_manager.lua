local DataManager = {}
DataManager.__index = DataManager

function DataManager.new()
  local self = setmetatable({}, DataManager)
  self.data = {}
  self.columns = {}
  self.primary_keys = {}
  self.changes = {}
  return self
end

function DataManager:set_data(result, primary_keys, columns)
  local rows = result
  if result and result.rows then
    rows = result.rows
    if result.columns then
      columns = result.columns
    end
  end
  
  self.data = rows
  self.primary_keys = primary_keys or {}
  self.changes = {}
  
  if columns and #columns > 0 then
    self.columns = columns
  elseif rows and #rows > 0 then
    self.columns = {}
    for key, _ in pairs(rows[1]) do
      table.insert(self.columns, key)
    end
  else
    self.columns = {}
  end
end

function DataManager:get_data()
  return self.data
end

function DataManager:get_columns()
  return self.columns
end

function DataManager:get_row(index)
  if index < 1 or index > #self.data then
    return nil
  end
  return self.data[index]
end

function DataManager:get_primary_key_values(row_index)
  if row_index < 1 or row_index > #self.data then
    return nil
  end
  
  local row = self.data[row_index]
  local pk_values = {}
  
  for _, pk in ipairs(self.primary_keys) do
    pk_values[pk] = row[pk]
  end
  
  return pk_values
end

function DataManager:add_change(row_index, field, value)
  if row_index < 1 or row_index > #self.data then
    error("Invalid row index")
  end
  
  local pk_values = self:get_primary_key_values(row_index)
  if not pk_values then
    error("Could not get primary key values for row")
  end
  
  if not self.changes[row_index] then
    self.changes[row_index] = {
      primary_key = pk_values,
      row_index = row_index,
      changes = {}
    }
  else
    self.changes[row_index].primary_key = pk_values
  end
  
  self.changes[row_index].changes[field] = value
end

function DataManager:remove_change(row_index)
  if row_index < 1 or row_index > #self.data then
    return false
  end
  
  self.changes[row_index] = nil
  return true
end

function DataManager:remove_cell_change(row_index, field)
  if row_index < 1 or row_index > #self.data then
    return false
  end
  
  local change = self.changes[row_index]
  if not change or not change.changes then
    return false
  end
  
  change.changes[field] = nil
  
  if next(change.changes) == nil then
    self.changes[row_index] = nil
  end
  
  return true
end

function DataManager:undo_row_changes(row_index)
  return self:remove_change(row_index)
end

function DataManager:get_change(row_index)
  if row_index < 1 or row_index > #self.data then
    return nil
  end
  
  return self.changes[row_index]
end

function DataManager:get_all_changes()
  return self.changes
end

function DataManager:get_change_by_row_index(row_index)
  if row_index < 1 or row_index > #self.data then
    return nil
  end
  
  return self.changes[row_index]
end

function DataManager:get_change_positions()
  local change_positions = {}
  for row_idx, change in pairs(self.changes) do
    if change and change.changes then
      if not change_positions[row_idx] then
        change_positions[row_idx] = {}
      end
      for field, _ in pairs(change.changes) do
        change_positions[row_idx][field] = true
      end
    end
  end
  return change_positions
end

function DataManager:has_changes()
  return next(self.changes) ~= nil
end

function DataManager:undo_table_changes()
  self.changes = {}
end

function DataManager:get_changed_row(row_index)
  local row = self:get_row(row_index)
  if not row then
    return nil
  end
  
  local change = self:get_change(row_index)
  if not change then
    return row
  end
  
  local changed_row = {}
  for key, value in pairs(row) do
    changed_row[key] = value
  end
  
  for field, value in pairs(change.changes) do
    changed_row[field] = value
  end
  
  return changed_row
end

function DataManager:get_changes_for_update()
  local updates = {}
  for row_idx, change in pairs(self.changes) do
    if change and change.primary_key and change.changes then
      table.insert(updates, {
        primary_key = change.primary_key,
        changes = change.changes
      })
    end
  end
  return updates
end

function DataManager:_get_edit_key(pk_values)
  local parts = {}
  for _, pk in ipairs(self.primary_keys) do
    table.insert(parts, tostring(pk_values[pk]))
  end
  return table.concat(parts, "::")
end

function DataManager:clear()
  self.data = {}
  self.columns = {}
  self.primary_keys = {}
  self.changes = {}
end

return DataManager

