import 'package:duckdbtest/sql_executor.dart';
import 'package:flutter/material.dart';

void main() => runApp(const MyApp());

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'DuckDB Explorer',
      theme: ThemeData(primarySwatch: Colors.blue),
      home: const SqlExecutorPage(),
    );
  }
}
