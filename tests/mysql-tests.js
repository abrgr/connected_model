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

module.exports.testSimpleInsert = function(test) {
    var testMain = function() {
        this.id = undefined;
        this.val = undefined;
    };

    var main = new testMain();
    main.id = 34;
    main.val = 9848;

    var mockQuery = new MonadTester('select', 'from', 'where', 'insert', 'update', 'set');
    
    mockQuery.insert('main', ['main.val'], [main.val]).EXPECT('execute').andCall(0).with(null, {warnings: undefined, id: main.id});

    var MockMain = new MySqlModel(testMain, newPool(mockQuery), {
        table: 'main',
        fields: {
            id: {id: true, field: 'id'},
            val: {field: 'val'}
        }
    });

    var mock = new MockMain();
    mock.val = main.val;

    mock.insert().success(function(result) {
        test.equal(main.id, result.id);
        test.done();
    }).fail(failTest.bind(null, test));
};

module.exports.testSaveAssociations = function(test) {
    var testAssoc = function() {
        this.testKey = undefined;
        this.testVal = undefined;
    };

    var testMain = function() {
        this.id = undefined;
        this.val = undefined;
        this.others = [];
    };

    var assoc = new testAssoc();
    assoc.testVal = 'test this';
    var ASSOC_ID = 375;

    var main = new testMain();
    main.id = 34;
    main.val = 9848;
    main.others = [assoc];

    var assocInsertDone = false;
    var linkingInsertDone = false;

    var mockQuery = new MonadTester('select', 'from', 'where', 'insert', 'update', 'set');
    
    mockQuery.select(['main.id', 'main.val']).from('main').where('id=?', [main.id]).EXPECT('execute').andCall(0).with(null, [main]);
    mockQuery.select(['assoc_id']).from('main_assoc').where('main_id=?', [main.id])
             .EXPECT('execute').andCall(0).with(null, []);
    mockQuery.select(['assoc.testKey', 'assoc.testVal']).from('assoc').where('assoc.testKey=?', [assoc.testKey])
             .EXPECT('execute').andCall(0).with(null, []);
    mockQuery.insert('assoc', ['assoc.testVal'], ['test this']).EXPECT('execute').andCall(function() {
        assocInsertDone = true;
    }, 0).with(null, {warnings: null, id: ASSOC_ID});

    var MockAssoc = new MySqlModel(testAssoc, newPool(mockQuery), {
        table: 'assoc',
        fields: {
            testKey: {id: true, field: 'testKey'},
            testVal: {field: 'testVal'}
        }
    });

    var MockMain = new MySqlModel(testMain, newPool(mockQuery), {
        table: 'main',
        fields: {
            id: {id: true, field: 'id'},
            val: {field: 'val'}
        },
        manyToManies: {others: {joinTable: 'main_assoc',
                                thisFields: {id: 'main_id'},
                                thatModel: MockAssoc,
                                thatFields: {testKey: 'assoc_id'}}}
    });

    main.saveAssociations().success(function(result) { 
        test.ok(assocInsertDone);
        test.done();
    }).fail(failTest.bind(null, test));
};
