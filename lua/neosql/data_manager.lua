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
  
  local change = self.changes[row_index]
  
  if change and change.status == "inserted" then
    change.changes[field] = value
    return
  end
  
  if change and change.status == "deleted" then
    change.status = "updated"
    change.changes = {}
    local pk_values = self:get_primary_key_values(row_index)
    if pk_values then
      change.primary_key = pk_values
    end
  end
  
  local pk_values = self:get_primary_key_values(row_index)
  if not pk_values then
    error("Could not get primary key values for row")
  end
  
  if not self.changes[row_index] then
    self.changes[row_index] = {
      primary_key = pk_values,
      row_index = row_index,
      status = "updated",
      changes = {}
    }
  else
    self.changes[row_index].primary_key = pk_values
    if not self.changes[row_index].status then
      self.changes[row_index].status = "updated"
    end
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

function DataManager:insert_new_row(insert_after_index)
  insert_after_index = insert_after_index or 0
  
  local new_row = {}
  for _, col in ipairs(self.columns) do
    new_row[col] = nil
  end
  
  local new_row_index = insert_after_index + 1
  table.insert(self.data, new_row_index, new_row)
  
  local updated_changes = {}
  for row_idx, change in pairs(self.changes) do
    if row_idx >= new_row_index then
      change.row_index = row_idx + 1
      updated_changes[row_idx + 1] = change
    else
      updated_changes[row_idx] = change
    end
  end
  self.changes = updated_changes
  
  self.changes[new_row_index] = {
    row_index = new_row_index,
    status = "inserted",
    changes = {}
  }
  
  return new_row_index
end

function DataManager:mark_row_deleted(row_index)
  if row_index < 1 or row_index > #self.data then
    return false
  end
  
  local change = self.changes[row_index]
  
  if change and change.status == "inserted" then
    table.remove(self.data, row_index)
    
    local updated_changes = {}
    for row_idx, chg in pairs(self.changes) do
      if row_idx < row_index then
        updated_changes[row_idx] = chg
      elseif row_idx > row_index then
        chg.row_index = row_idx - 1
        updated_changes[row_idx - 1] = chg
      end
    end
    self.changes = updated_changes
    
    return true
  end
  
  if change and change.status == "deleted" then
    self.changes[row_index] = nil
    return true
  end
  
  local pk_values = self:get_primary_key_values(row_index)
  if not pk_values then
    return false
  end
  
  if change and change.status == "updated" then
    self.changes[row_index] = {
      primary_key = pk_values,
      row_index = row_index,
      status = "deleted",
      changes = {}
    }
  else
    if not self.changes[row_index] then
      self.changes[row_index] = {
        primary_key = pk_values,
        row_index = row_index,
        status = "deleted",
        changes = {}
      }
    else
      self.changes[row_index].status = "deleted"
      self.changes[row_index].primary_key = pk_values
      self.changes[row_index].changes = {}
    end
  end
  
  return true
end

function DataManager:get_row_status(row_index)
  local change = self:get_change(row_index)
  if change and change.status then
    return change.status
  end
  return nil
end

function DataManager:get_changes_for_insert()
  local inserts = {}
  for row_idx, change in pairs(self.changes) do
    if change and change.status == "inserted" then
      local values = change.changes or {}
      if next(values) ~= nil then
        table.insert(inserts, {
          row_index = row_idx,
          values = values
        })
      end
    end
  end
  return inserts
end

function DataManager:get_changes_for_delete()
  local deletes = {}
  for row_idx, change in pairs(self.changes) do
    if change and change.status == "deleted" and change.primary_key then
      table.insert(deletes, {
        primary_key = change.primary_key
      })
    end
  end
  return deletes
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
    if change and change.status == "updated" and change.primary_key and change.changes then
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

