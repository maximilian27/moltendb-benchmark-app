# MoltenDB Benchmark App

A high-performance benchmarking application for **MoltenDB**, built with **Tauri** and **Rust**.

This application allows you to benchmark MoltenDB's performance across different storage modes (in-memory, async, and sync) with varying document counts (from 1000 to 5M). It provides real-time system metrics (CPU, RAM, Disk) and live query performance visualization.

## Features

- **Insert Data Benchmark**: Test insertion speeds for 1K to 5M documents.
- **Custom Query Runner**: Execute complex queries with filtering, projection, sorting, and pagination.
- **Live Metrics**: Monitor system resources while benchmarks are running.
- **Visual Performance**: Compare query execution times with interactive bar charts.
- **High Contrast Theme**: Optimized for readability with a sleek dark gray interface.

## Development

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install)
- [Node.js](https://nodejs.org/)
- Webview2 (for Windows)

### Running locally

The app can be run by downloading the pre-built binaries from the [Releases](https://github.com/maximilian27/moltendb-benchmark-app/releases) page.

> **Note for Windows Users:** Because this is a new, unsigned application, Microsoft Defender SmartScreen may display a "Windows protected your PC" warning when launching the `.exe`. To run the app, simply click **More info** and then **Run anyway**.

> **Note for macOS Users:** Because this app is unsigned, macOS Gatekeeper may block it from opening. To run the app, Right-click (or Control-click) the application icon and select Open.

> **Note for Linux Users:** If downloading the `.AppImage`, you must make it executable before running. Right-click the file, select Properties > Permissions, and check "Allow executing file as program" (or run `chmod +x` in your terminal).

Alternatively, you can run it from source:

1. Clone the repository.
2. Install dependencies:
   ```bash
   npm install
   ```
3. Run the app in development mode:
   ```bash
   npm run tauri dev
   ```

### Building

To build the production version:
```bash
npm run tauri build
```

## GitHub Workflow

This project includes a GitHub Action that automatically builds binaries for Linux, Windows, and macOS whenever code is pushed to the `master` branch. It also automatically bumps the minor version of the application.

## License

MIT
