/*
 * data_stream_processor.js
 * リアルタイムデータストリーム処理システム
 * 
 * 【処理概要】
 * イベントストリームをリアルタイムで処理・集計し、異常検知やトレンド分析を行うシステム
 * 
 * 【主な機能】
 * - イベント駆動アーキテクチャ（EventEmitter）
 * - ストリーム処理パイプライン（map, filter, reduce）
 * - 移動平均とトレンド分析
 * - 異常値検知アルゴリズム
 * - リアルタイム統計情報の集計
 * 
 * 【使用技術】
 * クラス、Promise、async/await、EventEmitter、ジェネレータ、クロージャ
 * 
 * 【実行方法】
 * node data_stream_processor.js
 */

const EventEmitter = require('events');
const fs = require('fs').promises;

// MARK: - カスタムエラークラス
// ストリーム処理中のエラーを表現
class StreamProcessorError extends Error {
    constructor(message, code) {
        super(message);
        this.name = 'StreamProcessorError';
        this.code = code;
    }
}

// MARK: - データポイント構造
// 個別のデータイベントを表現
class DataPoint {
    constructor(value, timestamp = new Date(), metadata = {}) {
        this.value = value;
        this.timestamp = timestamp;
        this.metadata = metadata;
        this.id = this._generateId();
    }
    
    // ユニークなIDを生成
    _generateId() {
        return `${this.timestamp.getTime()}-${Math.random().toString(36).substr(2, 9)}`;
    }
    
    // 有効なデータポイントかチェック
    isValid() {
        return typeof this.value === 'number' && !isNaN(this.value);
    }
}

// MARK: - ストリームプロセッサーベースクラス
// ストリーム処理の基本機能を提供
class StreamProcessor extends EventEmitter {
    constructor(name) {
        super();
        this.name = name;
        this.processedCount = 0;
        this.errorCount = 0;
    }
    
    // データポイントを処理（サブクラスでオーバーライド）
    async process(dataPoint) {
        throw new Error('process() must be implemented by subclass');
    }
    
    // エラーハンドリング
    handleError(error, dataPoint) {
        this.errorCount++;
        this.emit('error', { error, dataPoint, processor: this.name });
    }
    
    // 統計情報を取得
    getStats() {
        return {
            name: this.name,
            processed: this.processedCount,
            errors: this.errorCount
        };
    }
}

// MARK: - フィルタープロセッサー
// 条件に基づいてデータをフィルタリング
class FilterProcessor extends StreamProcessor {
    constructor(name, predicate) {
        super(name);
        // フィルタリング条件（関数）を保存
        this.predicate = predicate;
        this.filteredCount = 0;
    }
    
    async process(dataPoint) {
        this.processedCount++;
        
        try {
            // 条件チェック
            if (this.predicate(dataPoint)) {
                this.emit('data', dataPoint);
                return dataPoint;
            } else {
                this.filteredCount++;
                this.emit('filtered', dataPoint);
                return null;
            }
        } catch (error) {
            this.handleError(error, dataPoint);
            return null;
        }
    }
    
    getStats() {
        return {
            ...super.getStats(),
            filtered: this.filteredCount
        };
    }
}

// MARK: - 変換プロセッサー
// データを変換する
class TransformProcessor extends StreamProcessor {
    constructor(name, transformer) {
        super(name);
        // 変換関数を保存
        this.transformer = transformer;
    }
    
    async process(dataPoint) {
        this.processedCount++;
        
        try {
            // データを変換
            const transformed = await this.transformer(dataPoint);
            this.emit('data', transformed);
            return transformed;
        } catch (error) {
            this.handleError(error, dataPoint);
            return null;
        }
    }
}

// MARK: - 集計プロセッサー
// データを集計してウィンドウ単位で統計を計算
class AggregationProcessor extends StreamProcessor {
    constructor(name, windowSize = 10) {
        super(name);
        this.windowSize = windowSize;
        this.window = [];
    }
    
    async process(dataPoint) {
        this.processedCount++;
        
        try {
            // ウィンドウにデータを追加
            this.window.push(dataPoint);
            
            // ウィンドウサイズを超えたら古いデータを削除
            if (this.window.length > this.windowSize) {
                this.window.shift();
            }
            
            // 統計を計算
            const stats = this._calculateStats();
            this.emit('aggregation', stats);
            
            return stats;
        } catch (error) {
            this.handleError(error, dataPoint);
            return null;
        }
    }
    
    // ウィンドウ内のデータから統計を計算
    _calculateStats() {
        const values = this.window.map(dp => dp.value);
        const sum = values.reduce((a, b) => a + b, 0);
        const mean = sum / values.length;
        
        // 標準偏差を計算
        const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
        const stdDev = Math.sqrt(variance);
        
        return {
            count: values.length,
            sum: sum,
            mean: mean,
            min: Math.min(...values),
            max: Math.max(...values),
            stdDev: stdDev,
            timestamp: new Date()
        };
    }
}

// MARK: - 異常検知プロセッサー
// 統計的手法で異常値を検出
class AnomalyDetector extends StreamProcessor {
    constructor(name, threshold = 3) {
        super(name);
        this.threshold = threshold; // 標準偏差の何倍を異常とするか
        this.history = [];
        this.historySize = 50;
        this.anomalyCount = 0;
    }
    
    async process(dataPoint) {
        this.processedCount++;
        
        try {
            // 履歴にデータを追加
            this.history.push(dataPoint.value);
            if (this.history.length > this.historySize) {
                this.history.shift();
            }
            
            // 履歴が十分にある場合のみ異常検知
            if (this.history.length >= 10) {
                const isAnomaly = this._detectAnomaly(dataPoint.value);
                
                if (isAnomaly) {
                    this.anomalyCount++;
                    this.emit('anomaly', {
                        dataPoint: dataPoint,
                        stats: this._getHistoryStats()
                    });
                    console.log(`⚠️  異常値検出: ${dataPoint.value.toFixed(2)} (ID: ${dataPoint.id})`);
                }
            }
            
            this.emit('data', dataPoint);
            return dataPoint;
            
        } catch (error) {
            this.handleError(error, dataPoint);
            return null;
        }
    }
    
    // 異常値かどうかを判定（Zスコア法）
    _detectAnomaly(value) {
        const stats = this._getHistoryStats();
        
        // Zスコアを計算（標準偏差が0の場合は異常なし）
        if (stats.stdDev === 0) return false;
        
        const zScore = Math.abs((value - stats.mean) / stats.stdDev);
        return zScore > this.threshold;
    }
    
    // 履歴データの統計を取得
    _getHistoryStats() {
        const mean = this.history.reduce((a, b) => a + b, 0) / this.history.length;
        const variance = this.history.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / this.history.length;
        const stdDev = Math.sqrt(variance);
        
        return { mean, stdDev, count: this.history.length };
    }
    
    getStats() {
        return {
            ...super.getStats(),
            anomalies: this.anomalyCount,
            anomalyRate: (this.anomalyCount / this.processedCount * 100).toFixed(2) + '%'
        };
    }
}

// MARK: - トレンド分析プロセッサー
// 移動平均を使ってトレンドを分析
class TrendAnalyzer extends StreamProcessor {
    constructor(name, windowSize = 20) {
        super(name);
        this.windowSize = windowSize;
        this.values = [];
    }
    
    async process(dataPoint) {
        this.processedCount++;
        
        try {
            // 値を追加
            this.values.push(dataPoint.value);
            if (this.values.length > this.windowSize) {
                this.values.shift();
            }
            
            // 移動平均を計算
            if (this.values.length >= 5) {
                const trend = this._analyzeTrend();
                this.emit('trend', trend);
            }
            
            this.emit('data', dataPoint);
            return dataPoint;
            
        } catch (error) {
            this.handleError(error, dataPoint);
            return null;
        }
    }
    
    // トレンドを分析
    _analyzeTrend() {
        // 単純移動平均を計算
        const sma = this.values.reduce((a, b) => a + b, 0) / this.values.length;
        
        // 最新値と移動平均を比較
        const latest = this.values[this.values.length - 1];
        const diff = latest - sma;
        const diffPercent = (diff / sma * 100).toFixed(2);
        
        // トレンド方向を判定
        let direction = 'stable';
        if (Math.abs(parseFloat(diffPercent)) > 5) {
            direction = diff > 0 ? 'upward' : 'downward';
        }
        
        return {
            sma: sma,
            latest: latest,
            diff: diff,
            diffPercent: diffPercent + '%',
            direction: direction,
            timestamp: new Date()
        };
    }
}

// MARK: - データストリームパイプライン
// 複数のプロセッサーをチェーン接続
class DataStreamPipeline extends EventEmitter {
    constructor() {
        super();
        this.processors = [];
        this.isRunning = false;
    }
    
    // プロセッサーを追加
    addProcessor(processor) {
        this.processors.push(processor);
        
        // プロセッサーのイベントをパイプライン全体に伝播
        processor.on('error', (error) => this.emit('processorError', error));
        processor.on('anomaly', (data) => this.emit('anomaly', data));
        processor.on('trend', (data) => this.emit('trend', data));
        processor.on('aggregation', (data) => this.emit('aggregation', data));
        
        return this;
    }
    
    // データをパイプラインに投入
    async push(dataPoint) {
        if (!this.isRunning) {
            throw new StreamProcessorError('Pipeline is not running', 'NOT_RUNNING');
        }
        
        let current = dataPoint;
        
        // 各プロセッサーを順番に実行
        for (const processor of this.processors) {
            if (current === null) break;
            current = await processor.process(current);
        }
        
        this.emit('processed', current);
        return current;
    }
    
    // パイプラインを開始
    start() {
        this.isRunning = true;
        console.log('🚀 パイプライン開始');
        this.emit('start');
    }
    
    // パイプラインを停止
    stop() {
        this.isRunning = false;
        console.log('🛑 パイプライン停止');
        this.emit('stop');
    }
    
    // 統計情報を取得
    getStats() {
        return this.processors.map(p => p.getStats());
    }
    
    // 統計情報を表示
    printStats() {
        console.log('\n' + '='.repeat(70));
        console.log('📊 パイプライン統計');
        console.log('='.repeat(70));
        
        this.getStats().forEach((stats, index) => {
            console.log(`\n【${index + 1}. ${stats.name}】`);
            Object.entries(stats).forEach(([key, value]) => {
                if (key !== 'name') {
                    console.log(`  ${key}: ${value}`);
                }
            });
        });
        
        console.log('='.repeat(70));
    }
}

// MARK: - データジェネレータ
// テスト用のストリームデータを生成
class DataGenerator {
    constructor(config = {}) {
        this.baseValue = config.baseValue || 100;
        this.volatility = config.volatility || 10;
        this.trendRate = config.trendRate || 0.1;
        this.anomalyProbability = config.anomalyProbability || 0.05;
        this.currentValue = this.baseValue;
    }
    
    // 次のデータポイントを生成
    generate() {
        // トレンド成分を追加
        this.currentValue += this.trendRate;
        
        // ランダムなノイズを追加
        const noise = (Math.random() - 0.5) * this.volatility;
        let value = this.currentValue + noise;
        
        // 一定確率で異常値を生成
        if (Math.random() < this.anomalyProbability) {
            value += (Math.random() - 0.5) * this.volatility * 5;
        }
        
        return new DataPoint(value, new Date(), {
            source: 'generator',
            trend: this.trendRate,
            volatility: this.volatility
        });
    }
    
    // ジェネレータ関数でストリームを生成
    *stream(count) {
        for (let i = 0; i < count; i++) {
            yield this.generate();
        }
    }
}

// MARK: - 結果エクスポート機能
// 処理結果をJSONファイルに保存
class ResultExporter {
    constructor(filename) {
        this.filename = filename;
        this.results = [];
    }
    
    // 結果を追加
    addResult(type, data) {
        this.results.push({
            type: type,
            data: data,
            timestamp: new Date()
        });
    }
    
    // ファイルに保存
    async save() {
        try {
            const json = JSON.stringify(this.results, null, 2);
            await fs.writeFile(this.filename, json);
            console.log(`💾 結果を保存: ${this.filename}`);
        } catch (error) {
            console.error(`保存エラー: ${error.message}`);
        }
    }
}

// MARK: - メイン実行部分
async function main() {
    console.log('🚀 データストリーム処理システム起動\n');
    
    // パイプラインを構築
    const pipeline = new DataStreamPipeline();
    
    // プロセッサーを追加（チェーン形式）
    pipeline
        .addProcessor(new FilterProcessor('有効値フィルター', dp => dp.isValid()))
        .addProcessor(new TransformProcessor('正規化', dp => {
            // データを0-1の範囲に正規化（簡易版）
            dp.normalizedValue = dp.value / 200;
            return dp;
        }))
        .addProcessor(new AnomalyDetector('異常検知', 2.5))
        .addProcessor(new TrendAnalyzer('トレンド分析', 15))
        .addProcessor(new AggregationProcessor('集計', 20));
    
    // 結果エクスポーターを作成
    const exporter = new ResultExporter('stream_results.json');
    
    // イベントリスナーを設定
    pipeline.on('anomaly', (data) => {
        exporter.addResult('anomaly', data);
    });
    
    pipeline.on('trend', (data) => {
        if (data.direction !== 'stable') {
            console.log(`📈 トレンド: ${data.direction} (差分: ${data.diffPercent})`);
            exporter.addResult('trend', data);
        }
    });
    
    pipeline.on('aggregation', (data) => {
        if (data.count % 20 === 0) { // 20件ごとに表示
            console.log(`📊 集計 - 平均: ${data.mean.toFixed(2)}, 標準偏差: ${data.stdDev.toFixed(2)}`);
        }
    });
    
    pipeline.on('processorError', (error) => {
        console.error(`❌ プロセッサーエラー: ${error.error.message}`);
    });
    
    // データジェネレータを作成
    const generator = new DataGenerator({
        baseValue: 100,
        volatility: 15,
        trendRate: 0.2,
        anomalyProbability: 0.08
    });
    
    // パイプラインを開始
    pipeline.start();
    
    console.log('📡 データストリーム処理中...\n');
    
    // データを生成して処理（非同期で順次処理）
    const dataCount = 100;
    let processedCount = 0;
    
    for (const dataPoint of generator.stream(dataCount)) {
        // データをパイプラインに投入
        await pipeline.push(dataPoint);
        processedCount++;
        
        // 進捗表示（10件ごと）
        if (processedCount % 10 === 0) {
            console.log(`⏳ 進捗: ${processedCount}/${dataCount} 件処理完了`);
        }
        
        // 少し待機（リアルタイム処理をシミュレート）
        await new Promise(resolve => setTimeout(resolve, 50));
    }
    
    // パイプラインを停止
    pipeline.stop();
    
    // 統計情報を表示
    pipeline.printStats();
    
    // 結果を保存
    await exporter.save();
    
    console.log('\n🎉 処理が正常に完了しました！');
}

// エラーハンドリング付きでメイン関数を実行
main().catch(error => {
    console.error('❌ 予期しないエラー:', error);
    process.exit(1);
});
