# PV Scripts

Use pvlib for extracting GHI (Global Horizontal Irradiance) and PVOUT (Photovoltaic Output) for various sites in Crete.

## Requirements

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) package manager

## Installation

### 1. Install uv (if not already installed)

```bash
# On macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with pip
pip install uv

# Or with brew (macOS)
brew install uv
```

### 2. Clone and setup the project

```bash
# Clone the repository
git clone <repository-url>
cd pv-scripts

# Install dependencies and create virtual environment
uv sync
```

This will automatically:
- Create a virtual environment with Python 3.10
- Install all required dependencies from `pyproject.toml`
- Use the locked versions from `uv.lock` for reproducible builds

## Usage

### Running scripts with uv

```bash
# Run the main script
uv run python get_ghi_pvout_pvlib.py
```

### Alternative: Activate environment manually

```bash
# Activate the virtual environment
source .venv/bin/activate  # On macOS/Linux
# .venv\Scripts\activate   # On Windows

# Run scripts normally
python get_ghi_pvout_pvlib.py

# Deactivate when done
deactivate
```

## Dependencies

This project uses the following main libraries:
- **pvlib** (≥0.13.0) - Solar energy modeling library
- **pandas** (≥2.3.1) - Data analysis and manipulation
- **numpy** (≥2.2.6) - Numerical computing
- **matplotlib** (≥3.10.5) - Plotting and visualization
- **requests** (≥2.32.4) - HTTP library for data fetching
- **tqdm** (≥4.67.1) - Progress bars

## Development

### Adding new dependencies

```bash
# Add a new dependency
uv add <package-name>

# Add a development dependency
uv add --dev <package-name>
```

### Updating dependencies

```bash
# Update all dependencies
uv sync --upgrade

# Update a specific package
uv add <package-name>@latest
```

## Project Structure

```
pv-scripts/
├── get_ghi_pvout_pvlib.py  # Main script
├── pyproject.toml          # Project configuration and dependencies
├── uv.lock                 # Locked dependency versions
├── .python-version         # Python version specification
└── README.md               # This file
```

## Troubleshooting

- **Python version issues**: Make sure you have Python 3.10+ installed
- **uv not found**: Restart your terminal after installing uv, or add it to your PATH
- **Permission errors**: Use `uv sync --no-cache` to bypass cache issues

For more information about uv, visit the [official documentation](https://docs.astral.sh/uv/).
