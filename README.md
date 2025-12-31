# neosql

A Neovim plugin for PostgreSQL database interaction with an intuitive interface for querying, editing, and exporting data.
<img width="1470" height="893" alt="Screenshot 2025-12-31 at 18 08 05" src="https://github.com/user-attachments/assets/9f690be1-94ce-42a3-8e6b-6e38f49ce159" />


## Features

- **Query Execution**: Execute SQL queries and view results in a formatted markdown table
- **Data Editing**: Edit cell values directly in the result view
- **Row Management**: Insert new rows, update existing rows, and delete rows
- **Change Management**: Apply, undo, or clear changes before committing to the database
- **Export Support**: Export query results to CSV or JSON formats
- **Syntax Highlighting**: Visual highlighting for NULL values, boolean values, edited cells, and deleted rows
- **Primary Key Detection**: Automatically detects primary keys for safe data updates and deletes
- **Table Navigation**: Browse database tables and quickly generate SQL templates
- **SQL Templates**: Quick access to INSERT, SELECT, UPDATE, and DELETE templates from the table list

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
      insert_row = "i",
      delete_row = "dd",
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
- `i` - Insert new empty row after current row
- `dd` - Delete current row (toggle: press again to undo deletion)
- `a` - Apply all changes to database (inserts, updates, and deletes)
- `c` - Clear all changes (undo all edits)
- `u` - Undo change for current cell
- `U` - Undo all changes for current row
- `s` - Export results (prompts for filepath, detects format from extension)
- `q` - Focus query window
- `t` - Focus table list window

#### Table List Window
- `<CR>` - Select table and generate `SELECT * FROM "table_name" LIMIT 100;` query
- `i` - Insert template: `INSERT INTO "table_name" (?) VALUES (?);`
- `s` - Select template: `SELECT ? FROM "table_name" WHERE ?;`
- `u` - Update template: `UPDATE "table_name" SET ? = ? WHERE ?;`
- `d` - Delete template: `DELETE FROM "table_name" WHERE ?;`
- `q` - Focus query window
- `e` - Focus result window

### Editing Data

#### Updating Existing Rows

1. Execute a SELECT query to view data
2. Navigate to the result window and position your cursor on the cell you want to edit
3. Press `e` to edit the cell
4. Enter the new value (the plugin will attempt to preserve the data type)
5. Press `a` to apply all changes to the database

#### Inserting New Rows

1. Position your cursor on the row where you want to insert a new row
2. Press `i` to insert a new empty row after the current row
3. Edit the cells in the new row using `e`
4. Press `a` to apply all changes (the new row will be inserted into the database)

#### Deleting Rows

1. Position your cursor on the row you want to delete
2. Press `dd` to mark the row for deletion (the row will be highlighted with strikethrough)
3. Press `dd` again on the same row to undo the deletion
4. Press `a` to apply all changes (marked rows will be deleted from the database)

**Note**: The plugin automatically detects primary keys for the queried table. Updates and deletes are applied using WHERE clauses based on primary key values. If you edit a cell in a deleted row, the deletion is automatically undone and the row becomes an update instead.

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
- **Deleted rows**: Highlighted with strikethrough in gray

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
