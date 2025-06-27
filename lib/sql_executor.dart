import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'package:dart_duckdb/dart_duckdb.dart';
import 'package:device_info_plus/device_info_plus.dart';
import 'package:file_picker/file_picker.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:path_provider/path_provider.dart';
import 'package:flutter/material.dart';
import 'package:archive/archive.dart';
import 'package:path/path.dart' as p;

class SqlExecutorPage extends StatefulWidget {
  const SqlExecutorPage({super.key});

  @override
  _SqlExecutorPageState createState() => _SqlExecutorPageState();
}

typedef QueryResponse = ({
  List<String>? columns,
  List<List<Object?>>? rows,
  int totalRows,
});

class _SqlExecutorPageState extends State<SqlExecutorPage> {
  final _sqlController = TextEditingController();
  final _executionTimeController = TextEditingController();
  final _totalRowCountController = TextEditingController();
  final _currentPageController = TextEditingController();
  final _horizontalScrollController = ScrollController();
  final _verticalScrollController = ScrollController();

  Database? _database;
  Connection? _connection;
  List<String> _columnNames = [];
  List<List<dynamic>> _rows = [];
  final int _limit = 5;
  int _offset = 0;
  int _totalRows = 0;
  // Order-preserving list of tables imported from base.zip
  final List<String> _loadedTableNames = [];

  @override
  void initState() {
    super.initState();
  }

  @override
  void dispose() {
    _horizontalScrollController.dispose();
    _verticalScrollController.dispose();
    super.dispose();
  }

  Future<void> _openDatabaseWithEncryption() async {
    _askAllFilesPermission();
    final picked = await FilePicker.platform.pickFiles();
    final parquetPath = picked?.files.single.path;
    if (parquetPath == null) return;

    // 1. clean up the previous session
    await _connection?.dispose();
    await _database?.dispose();

    // 2. open (or create) an on-disk DB
    final docsDir = await getApplicationDocumentsDirectory();
    final dbPath = '${docsDir.path}/mydb.duckdb';
    _database = await duckdb.open(dbPath);

    // 3. **must** create a fresh connection
    _connection = await duckdb.connect(_database!);

    // 4. register the decryption key in this session
    await _connection!.query('''
    PRAGMA add_parquet_key(
      'key_ed7f46764697124d',
      'FnH27hGkQkYpl0aFlfTTkT+liCXOYuJgytn8s2PkXBM='
    );
  ''');

    // 5. import the encrypted Parquet (or use a view if you prefer)
    await _connection!.query('''
    CREATE TABLE IF NOT EXISTS encrypted_iris AS
    SELECT *
    FROM read_parquet(
      '$parquetPath',
      encryption_config = { footer_key: 'key_ed7f46764697124d' }
    );
  ''');

    // 6. show first page
    _sqlController.text = 'SELECT * FROM encrypted_iris';
    _offset = 0;
    await _executeQuery();

    setState(() {});
  }

  Future<bool> _ensureStoragePermission() async {
    // iOS & desktop never need it
    if (!Platform.isAndroid) return true;

    // Android 13 (API 33) and up: the SAF picker already grants read access
    final sdk = (await DeviceInfoPlugin().androidInfo).version.sdkInt;
    if (sdk >= 33) return true;

    // Android 11–12 → MANAGE_EXTERNAL_STORAGE
    if (sdk >= 30) {
      final status = await Permission.manageExternalStorage.request();
      return status.isGranted;
    }

    // Android 10 and below → READ/WRITE_EXTERNAL_STORAGE (grouped as Permission.storage)
    final status = await Permission.storage.request();
    return status.isGranted;
  }

  Future<void> _executeQuery({bool resetOffset = true}) async {
    if (_connection == null || _sqlController.text.trim().isEmpty) return;

    try {
      if (resetOffset) _offset = 0;
      final stopwatch = Stopwatch()..start();

      String sql = _sqlController.text.trim();
      sql = sql.replaceAll(RegExp(r';+$'), '');

      String countSql = '';
      if (sql.toLowerCase().startsWith('select')) {
        countSql = 'SELECT COUNT(*) FROM ($sql) AS count_query';
        sql += ' LIMIT $_limit OFFSET $_offset';
      }

      final taskParams = _QueryTaskParams(
        transferableDb: _database!.transferable,
        query: sql,
        countQuery: countSql,
        sendPort: null,
      );
      final receivePort = ReceivePort();
      final completer = Completer<void>();

      await Isolate.spawn<_QueryTaskParams>(
        _backgroundQueryTask,
        taskParams.copyWith(sendPort: receivePort.sendPort),
      );

      receivePort.listen((msg) {
        if (msg is QueryResponse) {
          setState(() {
            _columnNames = msg.columns ?? [];
            _rows = msg.rows ?? [];
            _totalRows = msg.totalRows;
            stopwatch.stop();
            _executionTimeController.text =
                '${stopwatch.elapsedMilliseconds} ms';
            _totalRowCountController.text = 'Total: $_totalRows';
            _currentPageController.text =
                'Page: ${(_offset / _limit).ceil() + 1}';
          });
          completer.complete();
        } else if (msg is String) {
          ScaffoldMessenger.of(context)
              .showSnackBar(SnackBar(content: Text('Error: $msg')));
          completer.completeError(msg);
          print('Error: $msg');
        }
      });

      await completer.future;
    } catch (e) {
      ScaffoldMessenger.of(context)
          .showSnackBar(SnackBar(content: Text('Error: $e')));
      print('Error: $e');
    }
  }

  Future<void> _loadMultipleEncryptedParquet() async {
    // 1) Ensure storage permission
    if (!await _ensureStoragePermission()) return;

    // 2) Pick up to two encrypted Parquet files
    final result = await FilePicker.platform.pickFiles(
      type: FileType.any,
      allowMultiple: true,
    );
    if (result == null || result.files.isEmpty) return;

    // 3) Reinitialize in-memory DuckDB
    _connection?.dispose();
    _database?.dispose();
    // _database = await duckdb.open(':memory:');

    final docsDir = await getApplicationDocumentsDirectory();
    _database = await duckdb.open('${docsDir.path}/mydb.duckdb');

    _connection = await duckdb.connect(_database!);

    // 4) Register both decryption keys
    await _connection!.query("""
    PRAGMA add_parquet_key(
      'key_9eb31dbfd603cd7e',
      'UT2lBL+XjYEPiZq51JjMCZzegWWTVWIFsIueGHn3qIE=
'
    );
    PRAGMA add_parquet_key(
      'housing_key_base64',
      'aNJBE90L0C5ebOMTZqrelmb/xG7CeIEZ0mP5RxPbWsI='
    );
  """);

    // 5) Import the first file
    final file1 = result.files[0];
    final path1 = file1.path!;
    final table1 =
        file1.name.split('.').first.replaceAll(RegExp(r'[^\w]+'), '_');
    await _connection!.query("""
    CREATE TABLE IF NOT EXISTS $table1 AS
    SELECT *
    FROM read_parquet(
      '$path1',
      encryption_config = { footer_key: 'key_ed7f46764697124d' }
    );
  """);

    // 6) If a second file was picked, import it too
    if (result.files.length > 1) {
      final file2 = result.files[1];
      final path2 = file2.path!;
      final table2 =
          file2.name.split('.').first.replaceAll(RegExp(r'[^\w]+'), '_');
      await _connection!.query("""
      CREATE TABLE IF NOT EXISTS $table2 AS
      SELECT *
      FROM read_parquet(
        '$path2',
        encryption_config = { footer_key: 'housing_key_base64' }
      );
    """);
    }

    // 7) Show all table names
    _sqlController.text = "SHOW TABLES";
    _offset = 0;
    await _executeQuery();

    setState(() {});
  }

  static void _backgroundQueryTask(_QueryTaskParams params) async {
    try {
      final conn = await duckdb.connectWithTransferred(params.transferableDb);
      final results = await conn.query(params.query);
      final cols = results.columnNames;
      final rows = results.fetchAll();
      int total = 0;
      if (params.countQuery.isNotEmpty) {
        total =
            (await conn.query(params.countQuery)).fetchAll().first.first as int;
      }
      params.sendPort!.send((columns: cols, rows: rows, totalRows: total));
    } catch (e) {
      params.sendPort!.send(e.toString());
    }
  }

  Future<void> _loadNextPage() async {
    setState(() => _offset += _limit);
    await _executeQuery(resetOffset: false);
  }

  Future<void> _loadPreviousPage() async {
    setState(() => _offset = (_offset - _limit).clamp(0, _offset));
    await _executeQuery(resetOffset: false);
  }

  Future<bool> _askAllFilesPermission() async {
    if (await Permission.manageExternalStorage.isGranted) return true;

    final status = await Permission.manageExternalStorage.request();
    return status.isGranted;
  }

  Future<void> _importZipAndLoadDbs() async {
    // 1. ensure storage permission
    if (!await _ensureStoragePermission()) return;

    // 2. pick the base ZIP
    final picked = await FilePicker.platform.pickFiles(
      type: FileType.custom,
      allowedExtensions: ['zip'],
    );
    if (picked == null || picked.files.isEmpty) return;
    final zipPath = picked.files.single.path!;

    // 3. unzip into app documents
    final docsDir = await getApplicationDocumentsDirectory();
    final archive = ZipDecoder().decodeBytes(File(zipPath).readAsBytesSync());

    // 4. extract known encrypted Parquet files
    final extracted = <_EncryptedParquetInfo, String>{};
    for (final file in archive) {
      if (!file.isFile) continue;
      final info = _knownEncryptedParquets.firstWhere(
        (e) => e.fileName == p.basename(file.name),
        orElse: () => const _EncryptedParquetInfo('', '', ''),
      );
      if (info.fileName.isEmpty) continue; // skip if not recognized

      final outPath = p.join(docsDir.path, info.fileName);
      File(outPath)
        ..createSync(recursive: true)
        ..writeAsBytesSync(file.content as List<int>);
      extracted[info] = outPath;
    }
    if (extracted.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('No recognized encrypted Parquet found.')),
      );
      return;
    }

    // 5. reinitialize DuckDB in-memory and import each Parquet as its own table
    await _connection?.dispose();
    await _database?.dispose();
    _database = await duckdb.open(':memory:');
    _connection = await duckdb.connect(_database!);
    _loadedTableNames.clear();

    for (final entry in extracted.entries) {
      final info = entry.key;
      final path = entry.value;
      await _connection!.query(
          "PRAGMA add_parquet_key('${info.keyName}', '${info.keyBase64}');");

      final tableName = p
          .basenameWithoutExtension(info.fileName)
          .replaceAll(RegExp(r'[^\w]+'), '_');
      await _connection!.query('''
      CREATE TABLE IF NOT EXISTS $tableName AS
      SELECT *
      FROM read_parquet(
        '$path',
        encryption_config = { footer_key: '${info.keyName}' }
      );
    ''');
      _loadedTableNames.add(tableName);
    }

    // 6. immediately show data from the first imported table
    if (_loadedTableNames.isNotEmpty) {
      _sqlController.text = 'SELECT * FROM ${_loadedTableNames.first};';
      _offset = 0;
      await _executeQuery();
    }

    setState(() {});
  }

  Future<void> _applyUpdateZip() async {
    // must have base data already loaded
    if (_connection == null) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Import base.zip first.')),
      );
      return;
    }

    // 1. pick the update ZIP
    if (!await _ensureStoragePermission()) return;
    final picked = await FilePicker.platform.pickFiles(
      type: FileType.custom,
      allowedExtensions: ['zip'],
    );
    if (picked == null || picked.files.isEmpty) return;
    final zipPath = picked.files.single.path!;

    // 2. unzip into app documents
    final docsDir = await getApplicationDocumentsDirectory();
    final archive = ZipDecoder().decodeBytes(File(zipPath).readAsBytesSync());

    // 3. extract known update Parquet files
    final updates = <_EncryptedParquetInfo, String>{};
    for (final file in archive) {
      if (!file.isFile) continue;
      final info = _updateEncryptedParquets.firstWhere(
        (e) => e.fileName == p.basename(file.name),
        orElse: () => const _EncryptedParquetInfo('', '', ''),
      );
      if (info.fileName.isEmpty) continue; // skip if not recognized

      final outPath = p.join(docsDir.path, info.fileName);
      File(outPath)
        ..createSync(recursive: true)
        ..writeAsBytesSync(file.content as List<int>);
      updates[info] = outPath;
    }
    if (updates.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('No recognized update files found.')),
      );
      return;
    }

    // 4. for each update file, register its key and INSERT rows into the matching base table
    try {
      for (final entry in updates.entries) {
        final info = entry.key;
        final path = entry.value;
        await _connection!.query(
            "PRAGMA add_parquet_key('${info.keyName}', '${info.keyBase64}');");

        final tableName = p
            .basenameWithoutExtension(info.fileName)
            .replaceAll(RegExp(r'[^\w]+'), '_');
        await _connection!.query('''
        INSERT INTO $tableName
        SELECT *
        FROM read_parquet(
          '$path',
          encryption_config = { footer_key: '${info.keyName}' }
        );
      ''');
      }
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Update applied.')),
      );
    } catch (e) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Error: $e')),
      );
    } finally {
      // 5. clean up temp files and the update.zip itself
      for (final pth in updates.values) {
        try {
          File(pth).deleteSync();
        } catch (__) {}
      }
      try {
        File(zipPath).deleteSync();
      } catch (__) {}
    }

    // 6. refresh the view of the first base table
    if (_loadedTableNames.isNotEmpty) {
      _sqlController.text = 'SELECT * FROM ${_loadedTableNames.first};';
      _offset = 0;
      await _executeQuery();
    }
    setState(() {});
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Colors.transparent,
      extendBodyBehindAppBar: true,
      appBar: AppBar(
        title: const Text('DuckDB Explorer'),
        actions: [
          IconButton(
            icon: const Icon(Icons.lock_open),
            tooltip: 'Import Encrypted Parquet',
            onPressed: _openDatabaseWithEncryption,
          ),
          IconButton(
            icon: const Icon(Icons.lock_clock),
            tooltip: 'Import Multiple Encrypted Parquet',
            onPressed: _loadMultipleEncryptedParquet,
          ),
          IconButton(
            icon: const Icon(Icons.archive),
            tooltip: 'Import zip',
            onPressed: _importZipAndLoadDbs,
          ),
          IconButton(
            icon: const Icon(Icons.archive),
            tooltip: 'update zip',
            onPressed: _applyUpdateZip,
          ),
        ],
      ),
      body: Stack(
        children: [
          Positioned.fill(
            child: Image.asset(
              'assets/bg.png',
              fit: BoxFit.cover,
              alignment: Alignment.topCenter,
            ),
          ),
          SafeArea(
            child: Column(
              children: [
                Padding(
                  padding: const EdgeInsets.all(8),
                  child: TextField(
                    controller: _sqlController,
                    decoration: const InputDecoration(
                      labelText: 'Enter SQL Query',
                      border: OutlineInputBorder(),
                    ),
                    maxLines: 3,
                  ),
                ),
                ElevatedButton(
                  onPressed: _executeQuery,
                  child: const Text('Run SQL'),
                ),
                Padding(
                  padding: const EdgeInsets.all(8),
                  child: Row(
                    children: [
                      Expanded(
                        child: TextField(
                          controller: _executionTimeController,
                          readOnly: true,
                          decoration: const InputDecoration(
                            labelText: 'Execution Time',
                            border: OutlineInputBorder(),
                          ),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: TextField(
                          controller: _totalRowCountController,
                          readOnly: true,
                          decoration: const InputDecoration(
                            labelText: 'Total Rows',
                            border: OutlineInputBorder(),
                          ),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: TextField(
                          controller: _currentPageController,
                          readOnly: true,
                          decoration: const InputDecoration(
                            labelText: 'Current Page',
                            border: OutlineInputBorder(),
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
                Expanded(child: _buildResultsTable()),
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    ElevatedButton(
                      onPressed: _offset > 0 ? _loadPreviousPage : null,
                      child: const Text('Previous'),
                    ),
                    ElevatedButton(
                      // enable while we still have at least one full page left
                      onPressed: (_offset + _limit) < _totalRows
                          ? _loadNextPage
                          : null,
                      child: const Text('Next'),
                    ),
                  ],
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildResultsTable() {
    if (_columnNames.isEmpty) {
      return const Center(child: Text('No results to display.'));
    }
    final columns =
        _columnNames.map((c) => DataColumn(label: Text(c))).toList();
    final rows = _rows.map((row) {
      return DataRow(
        cells: row.map((v) => DataCell(Text(v?.toString() ?? 'NULL'))).toList(),
      );
    }).toList();

    return Scrollbar(
      controller: _verticalScrollController,
      thumbVisibility: true,
      child: SingleChildScrollView(
        controller: _verticalScrollController,
        scrollDirection: Axis.vertical,
        child: Scrollbar(
          controller: _horizontalScrollController,
          thumbVisibility: true,
          child: SingleChildScrollView(
            controller: _horizontalScrollController,
            scrollDirection: Axis.horizontal,
            child: DataTable(columns: columns, rows: rows),
          ),
        ),
      ),
    );
  }
}

class _QueryTaskParams {
  final TransferableDatabase transferableDb;
  final String query, countQuery;
  final SendPort? sendPort;
  _QueryTaskParams({
    required this.transferableDb,
    required this.query,
    required this.countQuery,
    this.sendPort,
  });
  _QueryTaskParams copyWith({SendPort? sendPort}) => _QueryTaskParams(
        transferableDb: transferableDb,
        query: query,
        countQuery: countQuery,
        sendPort: sendPort ?? this.sendPort,
      );
}
// Add this after the QueryResponse typedef and before the _SqlExecutorPageState class

class EncryptionConfig {
  final String password;
  final bool enableEncryption;

  const EncryptionConfig({
    required this.password,
    this.enableEncryption = true,
  });
}

class _EncryptedParquetInfo {
  final String fileName; // fvrt_question_export.parquet
  final String keyName; // key_0a979db05312a981
  final String keyBase64; // NCxFAduK…
  const _EncryptedParquetInfo(this.fileName, this.keyName, this.keyBase64);
}

const List<_EncryptedParquetInfo> _knownEncryptedParquets = [
  _EncryptedParquetInfo(
    'fvrt_question_export.parquet',
    'key_0a979db05312a981',
    'NCxFAduKLvDO2oB3/ToqZp+rHmQ+26w7RAS5Y13IekY=',
  ),
  _EncryptedParquetInfo(
    'question_export.parquet',
    'key_962dba5f1d4ca724',
    'D37RqSFoqZvN7DHvRU8QFgz0XdmnVOTjEZfAybyWCx0=',
  ),
];

// put next to _knownEncryptedParquets
const List<_EncryptedParquetInfo> _updateEncryptedParquets = [
  _EncryptedParquetInfo(
    'fvrt_question_export.parquet',
    'key_558c1cd1ef12c154',
    '0VSUWDhvq9xh5IsKqAj0c7CKxVY58Tnf4dVWUJeju0s=',
  ),
  _EncryptedParquetInfo(
    'question_export.parquet',
    'key_eba2fc2d7a25783c',
    'NqZvdbZPrymUMakXoLHDJapOp/chXLw75gA8sx1Omwg=',
  ),
];
