local AppManager = require('neosql.app_manager')

local M = {}
M.app_manager = nil

function M.setup(opts)
  opts = opts or {}
  
  M.app_manager = AppManager.new()
  
  M.app_manager:setup_commands()
  
  if opts.bindings ~= false then
    local binding_opts = {
      enabled = opts.bindings ~= false,
      query = opts.bindings and opts.bindings.query,
      result = opts.bindings and opts.bindings.result,
      table_list = opts.bindings and opts.bindings.table_list,
    }
    M.app_manager:setup_bindings(binding_opts)
  end
end

function M.connect(config)
  if not M.app_manager then
    error("neosql not initialized. Call require('neosql').setup() first.")
  end
  return M.app_manager:connect(config)
end

function M.open()
  if not M.app_manager then
    error("neosql not initialized. Call require('neosql').setup() first.")
  end
  M.app_manager:open()
end

function M.close()
  if M.app_manager then
    M.app_manager:close()
  end
end

function M.get_app_manager()
  return M.app_manager
end

return M

