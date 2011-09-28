var MonadTester = require('./monad-tester');
var Deferred = require('deferred');

function failTest() {
    var test = this;
    test.ok(false);
    test.done();
}

module.exports.testSimple = function(test) {
    var expected1 = [{abc: 'abc', def: 'def'}, {abc: 'abc1', def: 'def1'}];
    var expected2 = [{abc: '123', def: '456'}, {abc: 'abc12', def: 'def12'}];

    var monad = new MonadTester('select', 'where', 'from').select('abc, def').from('table').where('x=?,y=?', [1, 34])
                                                          .EXPECT('execute').andReturn(Deferred.resolved(expected1));
    monad.select('a, b').from('abc join xyz').where('xyz=? AND this=?').EXPECT('execute').andReturn(Deferred.resolved(expected2));

    Deferred.afterAll(
        [
            monad.select('a, b').from('abc join xyz').where('xyz=? AND this=?').execute(),
            monad.select('abc, def').from('table').where('x=?,y=?', [1, 34]).execute()
        ]).success(function(results) {

        test.deepEqual(expected2, results[0][0]);
        test.deepEqual(expected1, results[1][0]);
        test.done();
    });
};

module.exports.testSimpleCallback = function(test) {
    var expected1 = [{abc: 'abc', def: 'def'}, {abc: 'abc1', def: 'def1'}];
    var expected2 = [{abc: '123', def: '456'}, {abc: 'abc12', def: 'def12'}];

    var monad = new MonadTester('select', 'where', 'from').select('abc, def').from('table').where('x=?,y=?', [1, 34])
                                                          .EXPECT('execute').andCall(0).with(expected1);
    monad.select('a, b').from('abc join xyz').where('xyz=? AND this=?').EXPECT('execute').andCall(0).with(expected2);

    var count = 0;
    monad.select('a, b').from('abc join xyz').where('xyz=? AND this=?').execute(function(actualRows) {
        test.deepEqual(expected2, actualRows);
        if ( ++count === 2 ) {
            test.done();
        }
    });

    monad.select('abc, def').from('table').where('x=?,y=?', [1, 34]).execute(function(actualRows) {
        test.deepEqual(expected1, actualRows);
        if ( ++count === 2 ) {
            test.done();
        }
    });
};

module.exports.testDoubleCallback = function(test) {
    var expected1 = [{abc: 'abc', def: 'def'}, {abc: 'abc1', def: 'def1'}];
    var expected2 = [{abc: '123', def: '456'}, {abc: 'abc12', def: 'def12'}];

    var gotIt1 = false;
    var gotIt2 = false;

    var monad = new MonadTester('select', 'where', 'from').select('abc, def').from('table').where('x=?,y=?', [1, 34])
                                                          .EXPECT('execute').andCall(function() { gotIt1 = true; }, 0).with(expected1);
    monad.select('a, b').from('abc join xyz').where('xyz=? AND this=?').EXPECT('execute').andCall(function() { gotIt2 = true; }, 0).with(expected2);

    var deferreds = [new Deferred(), new Deferred()];
    monad.select('a, b').from('abc join xyz').where('xyz=? AND this=?').execute(function(actualRows) {
        test.deepEqual(expected2, actualRows);
        deferreds[0].resolve();
    });

    monad.select('abc, def').from('table').where('x=?,y=?', [1, 34]).execute(function(actualRows) {
        test.deepEqual(expected1, actualRows);
        deferreds[1].resolve();
    });

    Deferred.afterAll(deferreds).success(function() {
        test.ok(gotIt1);
        test.ok(gotIt2);
        test.done();
    });
};
