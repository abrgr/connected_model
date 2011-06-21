var User = module.exports = function(id, name) {
    this.id = id;
    this.name = name;
};

User.prototype.is_valid = function() {
    return this.id > 0 && this.name && this.name.length > 3;
}

User.prototype.secureFn = function() {
    return this.id > 0 && this.name && this.name.length > 3;
}

User.prototype.secureFn.hide_from_client = true;

User.prototype.save = function() {
    return 'save done for: ' + this.name;
}

User.prototype.save.ajaxify=true;

/*User.get_by_id = function(id) {
    return new User(id, '' + id);
}

User.get_by_id.ajaxify = true;
*/

User.fromJSON = function(json) {
    return new User(json.id, json.name);
}
