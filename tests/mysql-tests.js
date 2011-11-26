var Db = require('db-mysql');
var _ = require('underscore');
var MySqlModel = require('./../lib/mysql-model');
var MonadTester = require('./monad-tester');

function failTest(test, err) {
    console.error(err);
    if ( err instanceof Error ) {
        console.error(err.message, err.stack);
    }
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

    var mockQuery = new MonadTester('select', 'from', 'where').select([{testTable_testKey: 'testTable.testKey'}, {testTable_testVal: 'testTable.testVal'}])
                                                              .from('testTable')
                                                              .where('testTable.testKey=?', ['key'])
                                                              .EXPECT('execute').andCall(0).with(null, [{testTable_testKey: expectedRows[0].testKey,
                                                                                                         testTable_testVal: expectedRows[0].testVal},
                                                                                                        {testTable_testKey: expectedRows[1].testKey,
                                                                                                         testTable_testVal: expectedRows[1].testVal}]);

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

    var mockQuery = new MonadTester('select', 'from', 'where').select([{testTable_testKey: 'testTable.testKey'}, {testTable_testVal: 'testTable.testVal'}])
                                                              .from('testTable')
                                                              .where('testTable.testKey=?', ['key'])
                                                              .EXPECT('execute').andCall(0).with(null, [{testTable_testKey: expected.testKey,
                                                                                                         testTable_testVal: expected.testVal}]);

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

var testSaveAssociations = function(test) {
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

    var ASSOC_JOIN_ID = 394;

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
    mockQuery.insert('main_assoc', ['main_id', 'assoc_id'], [main.id, ASSOC_ID]).EXPECT('execute').andCall(function() {
        linkingInsertDone = true;
    }, 0).with(null, {warnings: null, id: ASSOC_JOIN_ID});

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
        test.ok(assocInsertDone && linkingInsertDone);
        test.done();
    }).fail(failTest.bind(null, test));
};

module.exports.testJoinSelect = function(test) {
    var Medicine = function() {
        this.id = undefined;
        this.name = undefined;
        this.units = undefined;
    };

    var Prescription = function() {
        this.id = undefined;
        this.medicine = undefined;
        this.time = undefined;
        this.dose = undefined;
    };

    var advil = new Medicine();
    advil.id = 4;
    advil.name = 'Advil';
    advil.units = 'mg';

    var asprin = new Medicine();
    asprin.id = 8;
    asprin.name = 'Asprin';
    asprin.units = 'mg';

    var advilPrescription = new Prescription();
    advilPrescription.id = 84;
    advilPrescription.medicine = advil;
    advilPrescription.time = new Date();
    advilPrescription.dose = 874;

    var mockQuery = new MonadTester('select', 'where', 'join', 'from');

    mockQuery.select([{meds_id: 'meds.id'}, {meds_meds_info_id: 'meds.meds_info_id'}, {meds_dose: 'meds.dose'}, {meds_time: 'meds.time'}, 
                      {meds_info_id: 'meds_info.id'}, {meds_info_name: 'meds_info.name'}, {meds_info_units: 'meds_info.units'}])
             .from('meds')
             .join({table: 'meds_info', type: 'inner', conditions: 'meds.meds_info_id=meds_info.id'}, [])
             .where('meds.id=?', [advilPrescription.id])
             .EXPECT('execute').andCall(0).with(null, [{meds_id: advilPrescription.id, meds_info_id: advilPrescription.medicine.id, 
                                                        meds_info_name: advilPrescription.medicine.name,
                                                        meds_info_units: advilPrescription.medicine.units, meds_id: advilPrescription.id,
                                                        meds_dose: advilPrescription.dose, meds_time: advilPrescription.time}]);

    Medicine = new MySqlModel(Medicine, newPool(mockQuery),
    {
        table: 'meds_info',
        fields: {
            id: {field: 'id', id: true},
            name: {field: 'name'},
            units: {field: 'units'}
        }
    });

    Prescription = new MySqlModel(Prescription, newPool(mockQuery),
    {
        table: 'meds',
        fields: {
            id: {field: 'id', id: true},
            medicine: {field: 'meds_info_id', join: Medicine, joinType: 'inner'},
            dose: {field: 'dose'},
            time: {field: 'time'}
        }
    });

    Prescription.match({id: advilPrescription.id}).success(function(results) {
        test.deepEqual(advilPrescription, results[0]);
        test.done();
    }).fail(failTest.bind(null, test));
};

module.exports.testJoinInsert = function(test) {
    var Medicine = function() {
        this.id = undefined;
        this.name = undefined;
        this.units = undefined;
    };

    var Prescription = function() {
        this.id = undefined;
        this.medicine = undefined;
        this.time = undefined;
        this.dose = undefined;
    };

    var advil = new Medicine();
    advil.id = 4;
    advil.name = 'Advil';
    advil.units = 'mg';

    var advilPrescription = new Prescription();
    advilPrescription.medicine = advil;
    advilPrescription.time = new Date();
    advilPrescription.dose = 874;

    var mockQuery = new MonadTester('insert');

    mockQuery.insert('meds', ['meds_info_id', 'time', 'dose'], [advilPrescription.medicine.id, advilPrescription.time, advilPrescription.dose])
             .EXPECT('execute').andCall(0).with({id: 84}); 

    Medicine = new MySqlModel(Medicine, newPool(mockQuery),
    {
        table: 'meds_info',
        fields: {
            id: {field: 'id', id: true},
            name: {field: 'name'},
            units: {field: 'units'}
        }
    });

    Prescription = new MySqlModel(Prescription, newPool(mockQuery),
    {
        table: 'meds',
        fields: {
            id: {field: 'id', id: true},
            medicine: {field: 'meds_info_id', join: Medicine, joinType: 'inner'},
            dose: {field: 'dose'},
            time: {field: 'time'}
        }
    });

    Prescription.prototype.insert.call(advilPrescription).success(function(result) {
        test.deepEqual(result.id, 84);
        test.done();
    }).fail(failTest.bind(null, test));
};
