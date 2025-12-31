local Commands = {}
Commands.__index = Commands

function Commands.new(app_manager)
  local self = setmetatable({}, Commands)
  self.app_manager = app_manager
  return self
end

function Commands:register()
  vim.api.nvim_create_user_command("NeoSqlConnect", function(opts)
    local connection_string = opts.args
    if connection_string == "" then
      connection_string = vim.fn.input("Connection string: ", "", "file")
      if connection_string == "" then
        vim.notify("Connection cancelled", vim.log.levels.WARN)
        return
      end
    end

    local ok, err = self.app_manager:connect(connection_string)
    if not ok then
      vim.notify("Failed to connect: " .. tostring(err), vim.log.levels.ERROR)
      return
    end

    vim.notify("Connected successfully", vim.log.levels.INFO)
    self.app_manager:open()
  end, {
    nargs = "?",
    desc = "Connect to PostgreSQL database using connection string and open views",
    complete = "file",
  })

  vim.api.nvim_create_user_command("NeoSqlOpen", function()
    if not self.app_manager:is_connected() then
      vim.notify("Not connected to database. Use :NeoSqlConnect first", vim.log.levels.ERROR)
      return
    end

    self.app_manager:open()
  end, {
    desc = "Open neosql views (table list, query, result)",
  })

  vim.api.nvim_create_user_command("NeoSqlClose", function()
    self.app_manager:close()
    vim.notify("Neosql views closed", vim.log.levels.INFO)
  end, {
    desc = "Close neosql views",
  })

  vim.api.nvim_create_user_command("NeoSqlDisconnect", function()
    self.app_manager:disconnect()
    vim.notify("Disconnected from database", vim.log.levels.INFO)
  end, {
    desc = "Disconnect from PostgreSQL database",
  })
end

return Commands


