var User = module.exports = function(id, name) {
    this.id = id;
    this.name = name;
};

User.prototype.is_valid = function() {
    return this.id > 0 && this.name && this.name.length > 3;
};

User.prototype.secureFn = function() {
    return this.id > 0 && this.name && this.name.length > 3;
};

User.prototype.secureFn.hide_from_client = true;
