import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { ConfigError, parseConfig } from '../src/config/schema.js';
import { ConfigStore } from '../src/config/store.js';
import { baseConfig, makeStore } from './helpers.js';

test('环境变量展开：支持 ${VAR} 与 ${VAR:-default}', () => {
  const cfg = baseConfig([
    { id: 'a', adapter: 'openai', baseUrl: 'http://a.test/v1', apiKey: '${MY_KEY}', model: 'm' },
    { id: 'b', adapter: 'openai', baseUrl: '${MY_BASE:-http://b.test/v1}', apiKey: 'k', model: 'm' },
  ]);
  const parsed = parseConfig(cfg, { MY_KEY: 'sk-test' } as NodeJS.ProcessEnv);
  const targets = parsed.models['main']?.targets ?? [];
  assert.equal(targets[0]?.apiKey, 'sk-test');
  assert.equal(targets[1]?.baseUrl, 'http://b.test/v1');
});

test('缺失的环境变量会被一次性收集报错，而不是加载到一半才炸', () => {
  const cfg = baseConfig([
    { id: 'a', adapter: 'openai', baseUrl: 'http://a.test/v1', apiKey: '${MISSING_KEY}', model: 'm' },
    { id: 'a', adapter: 'openai', baseUrl: 'http://c.test/v1', apiKey: 'k', model: 'm' },
  ]);
  assert.throws(
    () => parseConfig(cfg, {} as NodeJS.ProcessEnv),
    (err: unknown) => {
      assert.ok(err instanceof ConfigError);
      assert.match(err.message, /MISSING_KEY/);
      assert.match(err.message, /is duplicated/);
      return true;
    },
  );
});

test('热重载：合法配置 → 版本号 +1，快照不可变', async () => {
  const { store, write } = await makeStore();
  try {
    assert.equal(store.configVersion, 0);
    assert.ok(Object.isFrozen(store.get().models));

    const next = baseConfig();
    (next['models'] as Record<string, { targets: Array<Record<string, unknown>> }>)['main']!.targets[1]!['model'] = 'model-b2';
    await write(next);

    const result = await store.reload();
    assert.equal(result.ok, true);
    assert.equal(result.configVersion, 1);
    assert.equal(store.get().models['main']?.targets[1]?.model, 'model-b2');
  } finally {
    store.close();
  }
});

test('热重载失败 → 保留旧配置（绝不半生效），错误可从 lastLoadError 读到', async () => {
  const { store, write, path } = await makeStore();
  try {
    const before = store.get();
    await writeFile(path, '{ this is not json', 'utf8');
    const result = await store.reload();

    assert.equal(result.ok, false);
    assert.equal(result.configVersion, 0);
    assert.equal(store.configVersion, 0);
    assert.equal(store.get(), before, '快照对象未被替换');
    assert.match(String(store.lastLoadError), /not valid JSON/);

    // 修好之后能恢复
    await write(baseConfig());
    const fixed = await store.reload();
    assert.equal(fixed.ok, true);
    assert.equal(fixed.configVersion, 1);
    assert.equal(store.lastLoadError, undefined);
  } finally {
    store.close();
  }
});

test('带 UTF-8 BOM 的配置文件也能加载（Windows 记事本常干这事）', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ai-gw-bom-'));
  const path = join(dir, 'bom.json');
  await writeFile(path, '\uFEFF' + JSON.stringify(baseConfig()), 'utf8');
  const store = await ConfigStore.open(path);
  try {
    assert.equal(store.get().defaultModel, 'main');
  } finally {
    store.close();
  }
});

test('文件变化会触发自动重载（watchFile 轮询路径）', async () => {
  const { store, write } = await makeStore();
  try {
    const seen: number[] = [];
    store.onChange((_cfg, version) => seen.push(version));

    const next = baseConfig();
    (next['defaultModel'] as string) === 'main';
    await write(next);

    const deadline = Date.now() + 4000;
    while (store.configVersion === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
    assert.equal(store.configVersion, 1);
    assert.deepEqual(seen, [1]);
  } finally {
    store.close();
  }
});
