# neosql

A Neovim plugin for PostgreSQL database interaction with an intuitive interface for querying, editing, and exporting data.

## Features

- **Query Execution**: Execute SQL queries and view results in a formatted markdown table
- **Data Editing**: Edit cell values directly in the result view
- **Change Management**: Apply, undo, or clear changes before committing to the database
- **Export Support**: Export query results to CSV or JSON formats
- **Syntax Highlighting**: Visual highlighting for NULL values, boolean values, and edited cells
- **Primary Key Detection**: Automatically detects primary keys for safe data updates
- **Table Navigation**: Browse database tables and quickly generate SELECT queries

## Installation

### Dependencies

This plugin requires `psql` (PostgreSQL command-line client) to be installed and available in your PATH. `psql` is typically included with PostgreSQL installations.

### Plugin Installation

Using lazy.nvim:

```lua
{
  "h4kbas/neosql",
  config = function()
    require('neosql').setup({
      -- configuration options
    })
  end
}
```

Using packer.nvim:

```lua
use 'h4kbas/neosql'
```

Using vim-plug:

```vim
Plug 'h4kbas/neosql'
```

## Configuration

```lua
require('neosql').setup({
  bindings = {
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
      focus_query = "q",
      focus_result = "e",
    },
  }
})
```

## Usage

### Commands

- `:NeoSqlConnect [connection_string]` - Connect to PostgreSQL database using connection string and open views. If no connection string is provided, you'll be prompted to enter one.
- `:NeoSqlOpen` - Open neosql views (table list, query, result)
- `:NeoSqlClose` - Close neosql views
- `:NeoSqlDisconnect` - Disconnect from PostgreSQL database

### Keybindings

#### Query Window
- `<CR>` - Execute query
- `e` - Focus result window
- `t` - Focus table list window

#### Result Window
- `e` - Edit cell at cursor position
- `a` - Apply all changes to database
- `c` - Clear all changes (undo all edits)
- `u` - Undo change for current cell
- `U` - Undo all changes for current row
- `s` - Export results (prompts for filepath, detects format from extension)
- `q` - Focus query window
- `t` - Focus table list window

#### Table List Window
- `<CR>` - Select table and generate `SELECT * FROM "table_name" LIMIT 100;` query
- `q` - Focus query window
- `e` - Focus result window

### Editing Data

1. Execute a SELECT query to view data
2. Navigate to the result window and position your cursor on the cell you want to edit
3. Press `e` to edit the cell
4. Enter the new value (the plugin will attempt to preserve the data type)
5. Press `a` to apply all changes to the database

**Note**: The plugin automatically detects primary keys for the queried table. Changes are applied using UPDATE statements with WHERE clauses based on primary key values.

### Exporting Data

1. Execute a query to view results
2. In the result window, press `s` to export
3. Enter a filepath with a `.csv` or `.json` extension
4. The plugin will automatically detect the format and export accordingly

Supported formats:
- **CSV**: Exports with proper escaping for commas, quotes, and newlines
- **JSON**: Exports as a JSON array of objects

### Visual Features

- **NULL values**: Highlighted in gray italic
- **Boolean values**: `t` (true) highlighted in cyan, `f` (false) highlighted in red
- **Edited cells**: Highlighted in blue bold

### Lua API

Connect to database:

Using connection string:

```lua
require('neosql').connect("postgresql://user:password@localhost:5432/database")
```

Or using config object:

```lua
require('neosql').connect({
  host = 'localhost',
  port = 5432,
  database = 'mydb',
  user = 'myuser',
  password = 'mypassword',
})
```

Open the interface:

```lua
require('neosql').open()
```

Close the interface:

```lua
require('neosql').close()
```

Get the app manager for advanced usage:

```lua
local app_manager = require('neosql').get_app_manager()
```
