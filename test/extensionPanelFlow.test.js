const fs = require('node:fs');

module.exports = function runExtensionPanelFlowTests(runTest, assert) {
  runTest('changeConnection from the error panel reruns the last query after a successful server update', () => {
    const compiledExtensionSource = fs.readFileSync(require.resolve('../out/extension'), 'utf8');

    assert.ok(
      compiledExtensionSource.includes("executeCommand('sqlDebugger.configureConnection')"),
    );
    assert.ok(
      compiledExtensionSource.includes('if (updated && lastRun) {\r\n                await rerunLastQueryInPanel(context, panel);')
      || compiledExtensionSource.includes('if (updated && lastRun) {\n                await rerunLastQueryInPanel(context, panel);')
    );
  });
};
