# MoltenDB Benchmark App

A high-performance benchmarking application for **MoltenDB**, built with **Tauri** and **Rust**.

This application allows you to benchmark MoltenDB's performance across different storage modes (in-memory, async, and sync) with varying document counts. It provides real-time system metrics (CPU, RAM, Disk) and live query performance visualization.

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

The app can be run by downloading the pre-built binaries from the [Releases](https://github.com/maximilian27/moltendb-benchmark-app/releases) page. Alternatively, you can run it from source:

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
