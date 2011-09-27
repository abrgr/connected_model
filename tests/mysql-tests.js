var Db = require('db-mysql');
var _ = require('underscore');
var MySqlModel = require('./../lib/mysql-model');
var MonadTester = require('./monad-tester');

function failTest(test, err) {
    console.error(err);
    console.error(err.message, err.stack);
    test.ok(false);
    test.done();
}

function MockDb(query) {
    return {
        query: function() { return query; },
        name: _.identity.bind(_)
    };
}

function newPool(query) {
    return {
        acquire: function(cb) { return cb(undefined, new MockDb(query)); },
        release: function() {}
    };
}

module.exports.testSimpleMatch = function(test) {
    var testCtor = function() {
        this.testKey = undefined;
        this.testVal = undefined;
    };

    var expectedRows = [{testKey: 'key', testVal: 1234}, {testKey: 'key', testVal: 743.4}];

    var mockQuery = new MonadTester('select', 'from', 'where').select(['testTable.testKey', 'testTable.testVal']).from('testTable')
                                                              .where('testTable.testKey=?', ['key'])
                                                              .EXPECT('execute').andCall(0).with(null, expectedRows);

    var Mock = new MySqlModel(testCtor, newPool(mockQuery), {
        table: 'testTable',
        fields: {
            testKey: {id: true, field: 'testKey'},
            testVal: {field: 'testVal'}
        }
    });

    Mock.match({testKey: 'key'}).success(function(actualRows) {
        test.deepEqual(expectedRows, actualRows);
        test.done();
    }).fail(failTest.bind(null, test));
};

module.exports.testSimpleMatchSingle = function(test) {
    var testCtor = function() {
        this.testKey = undefined;
        this.testVal = undefined;
    };

    var expected = {testKey: 'key', testVal: 1234};

    var mockQuery = new MonadTester('select', 'from', 'where').select(['testTable.testKey', 'testTable.testVal']).from('testTable')
                                                              .where('testTable.testKey=?', ['key'])
                                                              .EXPECT('execute').andCall(0).with(null, [expected]);

    var Mock = new MySqlModel(testCtor, newPool(mockQuery), {
        table: 'testTable',
        fields: {
            testKey: {id: true, field: 'testKey'},
            testVal: {field: 'testVal'}
        }
    });

    Mock.matchSingle({testKey: 'key'}).success(function(actualRows) {
        test.deepEqual(expected, actualRows);
        test.done();
    }).fail(failTest.bind(null, test));
};

module.exports.testSaveAssociations = function(test) {
    test.done();
};
