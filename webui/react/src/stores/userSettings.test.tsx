import { Loadable, Loaded } from 'determined-ui/utils/loadable';
import { Map } from 'immutable';
import * as t from 'io-ts';

import { getUserSetting } from 'services/api';
import { V1UserWebSetting } from 'services/api-ts-sdk';
import authStore from 'stores/auth';
import userStore from 'stores/users';

import { UserSettingsStore } from './userSettings';

const CURRENT_USER = { id: 1, isActive: true, isAdmin: false, username: 'bunny' };

vi.mock('services/api', () => ({
  getUserSetting: vi.fn(() => Promise.resolve({ settings: [] })),
  resetUserSetting: () => Promise.resolve(),
  updateUserSetting: () => Promise.resolve(),
}));

const Config = t.type({
  boolean: t.boolean,
  booleanArray: t.union([t.array(t.boolean), t.null]),
  number: t.union([t.null, t.number]),
  numberArray: t.array(t.number),
  string: t.union([t.null, t.string]),
  stringArray: t.union([t.null, t.array(t.string)]),
});
const configPath = 'settings-normal';

const setup = async (initialValue = {}) => {
  authStore.setAuth({ isAuthenticated: true });
  authStore.setAuthChecked();
  userStore.updateCurrentUser(CURRENT_USER);
  const store = new UserSettingsStore();
  await store.overwrite(Map(initialValue));
  return store;
};

describe('userSettings', () => {
  const expectedSettings = {
    boolean: false,
    booleanArray: [false, true],
    number: 3.14e-12,
    numberArray: [0, 100, -5280],
    string: 'Hello World',
    stringArray: ['abc', 'def', 'ghi'],
  };
  const expectedValue = 'henlo';

  afterEach(() => vi.clearAllMocks());

  describe('set', () => {
    it('interface should update settings when given an interface', async () => {
      const store = await setup();

      const oldSettings = store.get(Config, configPath).get();
      expect(oldSettings).not.toStrictEqual(Loaded(expectedSettings));
      store.set(Config, configPath, expectedSettings);
      const newSettings = store.get(Config, configPath).get();
      expect(newSettings).toStrictEqual(Loaded(expectedSettings));
    });

    it('basic should update settings when given a basic type', async () => {
      const store = await setup();

      const oldSettings = store.get(t.string, configPath).get();
      expect(oldSettings).not.toStrictEqual(Loaded(expectedValue));
      store.set(t.string, configPath, expectedValue);
      const newSettings = store.get(t.string, configPath).get();
      expect(newSettings).toStrictEqual(Loaded(expectedValue));
    });
  });

  describe('setPartial', () => {
    it('should accept partial updates', async () => {
      const store = await setup({ [configPath]: expectedSettings });

      store.setPartial(Config, configPath, { string: expectedValue });
      const result = store.get(Config, configPath).get();
      expect(Loadable.map(result, (r) => r?.string)).toStrictEqual(Loaded(expectedValue));
    });
    it('should work on semipartial types', async () => {
      const Semi = t.intersection([
        t.type({
          bar: t.string,
          foo: t.number,
        }),
        t.partial({
          baz: t.boolean,
          qux: t.array(t.number),
        }),
      ]);
      const path = 'semi';
      const init = { bar: 'one', baz: true, foo: 1 };
      const expected = 'two';
      const expected2 = [1];

      const store = await setup({ [path]: init });

      store.setPartial(Semi, path, { bar: expected });
      const result = store.get(Semi, path).get();
      expect(Loadable.map(result, (r) => r?.bar)).toStrictEqual(Loaded(expected));

      store.setPartial(Semi, path, { qux: expected2 });
      const result2 = store.get(Semi, path).get();
      expect(Loadable.map(result2, (r) => r?.qux)).toStrictEqual(Loaded(expected2));
    });
  });

  describe('update', () => {
    it('should update given an updater', async () => {
      const store = await setup({ [configPath]: expectedSettings });

      const spy = vi.fn((val) => ({ ...val, string: expectedValue }));
      store.update(Config, configPath, spy);
      expect(spy).toBeCalledTimes(1);
      const result = store.get(Config, configPath).get();
      expect(Loadable.map(result, (r) => r?.string)).toStrictEqual(Loaded(expectedValue));
      expect(Loadable.map(result, (r) => r?.stringArray)).toStrictEqual(
        Loaded(expectedSettings.stringArray),
      );
    });
  });

  describe('polling', () => {
    let store: UserSettingsStore;
    const getSpy = vi.mocked(getUserSetting);
    beforeEach(async () => {
      store = await setup({ [configPath]: expectedSettings });
      store.startPolling();
      // userSettings sets up a timer that we want to skip here
      await vi.advanceTimersToNextTimerAsync();
    });
    beforeAll(() => vi.useFakeTimers());

    afterEach(() => store.stopPolling());
    afterAll(() => vi.useRealTimers());

    it('should call getUserSettings once before starting to poll', () => {
      expect(getSpy).toBeCalledTimes(1);
    });

    it('should call getUserSettings after starting to poll', async () => {
      getSpy.mockClear();
      await vi.runOnlyPendingTimersAsync();
      expect(getSpy).toBeCalledTimes(1);
    });

    const pollWithSettings = async (...settings: V1UserWebSetting[]) => {
      getSpy.mockImplementationOnce(() => Promise.resolve({ settings }));
      await vi.runOnlyPendingTimersAsync();
    };
    it('should overwrite existing settings', async () => {
      // merge settings entries with existing settings
      const settings = store.get(Config, configPath);
      expect(Loadable.map(settings.get(), (o) => o?.boolean)).toStrictEqual(Loaded(false));

      await pollWithSettings({ key: 'boolean', storagePath: configPath, value: 'true' });
      expect(Loadable.map(settings.get(), (o) => o?.boolean)).toStrictEqual(Loaded(true));
    });

    it('should allow for flat settings values', async () => {
      // root values
      const settings = store.get(t.string, 'single_value');
      expect(settings.get()).toStrictEqual(Loaded(null));

      await pollWithSettings({ key: '_ROOT', storagePath: 'single_value', value: '"hello"' });
      expect(settings.get()).toStrictEqual(Loaded('hello'));
    });

    it('should create new objects if necessary', async () => {
      // create new objects
      const settings = store.get(t.interface({ value: t.string }), 'new_object');
      expect(settings.get()).toStrictEqual(Loaded(null));

      await pollWithSettings({ key: 'value', storagePath: 'new_object', value: '"hello"' });
      expect(Loadable.map(settings.get(), (s) => s?.value)).toStrictEqual(Loaded('hello'));
    });

    it('should keep object reference when poll returns same data', async () => {
      const settings = store.get(Config, configPath);
      const oldRef = settings.get();

      await pollWithSettings({ key: 'boolean', storagePath: configPath, value: 'false' });
      expect(settings.get()).toBe(oldRef);
    });

    it('should return new object reference when poll returns new data', async () => {
      const settings = store.get(Config, configPath);
      const oldRef = settings.get();

      await pollWithSettings({ key: 'boolean', storagePath: configPath, value: 'true' });
      expect(settings.get()).not.toBe(oldRef);
    });
  });
});
