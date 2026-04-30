const DataRowState = {
    DETACHED: 'DETACHED',
    ADDED: 'ADDED',
    MODIFIED: 'MODIFIED',
    DELETED: 'DELETED',
    UNCHANGED: 'UNCHANGED'
};

// Utility methods for state management
DataRowState.isChanged = function(state) {
    return state === this.ADDED || state === this.MODIFIED || state === this.DELETED;
};

DataRowState.isUnchanged = function(state) {
    return state === this.UNCHANGED;
};

DataRowState.isDetached = function(state) {
    return state === this.DETACHED;
};

module.exports = DataRowState;
