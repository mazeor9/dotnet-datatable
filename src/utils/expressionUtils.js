function compileExpression(expression) {
    if (typeof expression !== 'string') {
        throw new Error('Expression must be a string');
    }
    const tokens = tokenize(expression);
    const parser = new Parser(tokens);
    const ast = parser.parseExpression();
    parser.expect('eof');
    return function compiled(rowProxy, dataRow, table, _version) {
        return evaluate(ast, { rowProxy, dataRow, table });
    };
}

function compilePredicate(expression) {
    const fn = compileExpression(expression);
    return function predicate(rowProxy, dataRow, table) {
        return Boolean(fn(rowProxy, dataRow, table));
    };
}

function tokenize(input) {
    const text = String(input);
    const tokens = [];
    let i = 0;

    const isWs = (c) => /\s/.test(c);
    const isDigit = (c) => /[0-9]/.test(c);
    const isIdentStart = (c) => /[A-Za-z_]/.test(c);
    const isIdent = (c) => /[A-Za-z0-9_.$]/.test(c);

    while (i < text.length) {
        const c = text[i];
        if (isWs(c)) {
            i++;
            continue;
        }

        if (c === "'" || c === '"') {
            const quote = c;
            i++;
            let value = '';
            while (i < text.length) {
                const ch = text[i];
                if (ch === '\\') {
                    const next = text[i + 1];
                    if (next !== undefined) {
                        value += next;
                        i += 2;
                        continue;
                    }
                }
                if (ch === quote) {
                    i++;
                    break;
                }
                value += ch;
                i++;
            }
            tokens.push({ type: 'string', value });
            continue;
        }

        if (isDigit(c) || (c === '.' && isDigit(text[i + 1]))) {
            let start = i;
            i++;
            while (i < text.length && /[0-9.]/.test(text[i])) {
                i++;
            }
            const raw = text.slice(start, i);
            tokens.push({ type: 'number', value: Number(raw) });
            continue;
        }

        if (isIdentStart(c)) {
            let start = i;
            i++;
            while (i < text.length && isIdent(text[i])) {
                i++;
            }
            const raw = text.slice(start, i);
            const upper = raw.toUpperCase();
            if (upper === 'AND' || upper === 'OR' || upper === 'NOT') {
                tokens.push({ type: 'keyword', value: upper });
            } else if (upper === 'NULL') {
                tokens.push({ type: 'null', value: null });
            } else if (upper === 'TRUE') {
                tokens.push({ type: 'boolean', value: true });
            } else if (upper === 'FALSE') {
                tokens.push({ type: 'boolean', value: false });
            } else {
                tokens.push({ type: 'identifier', value: raw });
            }
            continue;
        }

        const two = text.slice(i, i + 2);
        const three = text.slice(i, i + 3);
        if (three === '===') {
            tokens.push({ type: 'operator', value: '===' });
            i += 3;
            continue;
        }
        if (three === '!==') {
            tokens.push({ type: 'operator', value: '!==' });
            i += 3;
            continue;
        }
        if (two === '&&' || two === '||' || two === '>=' || two === '<=' || two === '==' || two === '!=' || two === '<>') {
            tokens.push({ type: 'operator', value: two });
            i += 2;
            continue;
        }

        if ('+-*/%()!,<>=,'.includes(c)) {
            if (c === '(' || c === ')') {
                tokens.push({ type: 'paren', value: c });
            } else if (c === ',') {
                tokens.push({ type: 'comma', value: c });
            } else {
                tokens.push({ type: 'operator', value: c });
            }
            i++;
            continue;
        }

        throw new Error(`Unexpected character '${c}' in expression`);
    }

    tokens.push({ type: 'eof', value: null });
    return tokens;
}

class Parser {
    constructor(tokens) {
        this.tokens = tokens;
        this.pos = 0;
    }

    peek() {
        return this.tokens[this.pos];
    }

    next() {
        return this.tokens[this.pos++];
    }

    expect(type, value = undefined) {
        const token = this.next();
        if (!token || token.type !== type) {
            throw new Error(`Expected ${type}`);
        }
        if (value !== undefined && token.value !== value) {
            throw new Error(`Expected ${type} ${value}`);
        }
        return token;
    }

    parseExpression() {
        return this.parseOr();
    }

    parseOr() {
        let node = this.parseAnd();
        while (true) {
            const t = this.peek();
            if ((t.type === 'keyword' && t.value === 'OR') || (t.type === 'operator' && t.value === '||')) {
                this.next();
                node = { type: 'binary', op: 'OR', left: node, right: this.parseAnd() };
                continue;
            }
            return node;
        }
    }

    parseAnd() {
        let node = this.parseComparison();
        while (true) {
            const t = this.peek();
            if ((t.type === 'keyword' && t.value === 'AND') || (t.type === 'operator' && t.value === '&&')) {
                this.next();
                node = { type: 'binary', op: 'AND', left: node, right: this.parseComparison() };
                continue;
            }
            return node;
        }
    }

    parseComparison() {
        let node = this.parseAdd();
        while (true) {
            const t = this.peek();
            if (t.type === 'operator' && ['=', '==', '===', '!=', '!==', '<>', '>', '>=', '<', '<='].includes(t.value)) {
                this.next();
                node = { type: 'binary', op: t.value, left: node, right: this.parseAdd() };
                continue;
            }
            return node;
        }
    }

    parseAdd() {
        let node = this.parseMul();
        while (true) {
            const t = this.peek();
            if (t.type === 'operator' && (t.value === '+' || t.value === '-')) {
                this.next();
                node = { type: 'binary', op: t.value, left: node, right: this.parseMul() };
                continue;
            }
            return node;
        }
    }

    parseMul() {
        let node = this.parseUnary();
        while (true) {
            const t = this.peek();
            if (t.type === 'operator' && (t.value === '*' || t.value === '/' || t.value === '%')) {
                this.next();
                node = { type: 'binary', op: t.value, left: node, right: this.parseUnary() };
                continue;
            }
            return node;
        }
    }

    parseUnary() {
        const t = this.peek();
        if ((t.type === 'keyword' && t.value === 'NOT') || (t.type === 'operator' && (t.value === '!' || t.value === '-' || t.value === '+'))) {
            this.next();
            const op = t.type === 'keyword' ? 'NOT' : t.value;
            return { type: 'unary', op, expr: this.parseUnary() };
        }
        return this.parsePrimary();
    }

    parsePrimary() {
        const t = this.peek();
        if (t.type === 'number' || t.type === 'string' || t.type === 'boolean' || t.type === 'null') {
            this.next();
            return { type: 'literal', value: t.value };
        }
        if (t.type === 'identifier') {
            const name = this.next().value;
            const next = this.peek();
            if (next.type === 'paren' && next.value === '(') {
                this.next();
                const args = [];
                if (!(this.peek().type === 'paren' && this.peek().value === ')')) {
                    while (true) {
                        args.push(this.parseExpression());
                        if (this.peek().type === 'comma') {
                            this.next();
                            continue;
                        }
                        break;
                    }
                }
                this.expect('paren', ')');
                return { type: 'call', name, args };
            }
            return { type: 'identifier', name };
        }
        if (t.type === 'paren' && t.value === '(') {
            this.next();
            const expr = this.parseExpression();
            this.expect('paren', ')');
            return expr;
        }
        throw new Error('Unexpected token in expression');
    }
}

function evaluate(node, ctx) {
    switch (node.type) {
        case 'literal':
            return node.value;
        case 'identifier':
            return resolveIdentifier(ctx.rowProxy, node.name);
        case 'unary': {
            const v = evaluate(node.expr, ctx);
            switch (node.op) {
                case 'NOT':
                case '!':
                    return !truthy(v);
                case '-':
                    return -Number(v);
                case '+':
                    return Number(v);
                default:
                    return v;
            }
        }
        case 'binary': {
            if (node.op === 'AND') {
                return truthy(evaluate(node.left, ctx)) && truthy(evaluate(node.right, ctx));
            }
            if (node.op === 'OR') {
                return truthy(evaluate(node.left, ctx)) || truthy(evaluate(node.right, ctx));
            }
            const a = evaluate(node.left, ctx);
            const b = evaluate(node.right, ctx);
            return applyBinary(node.op, a, b);
        }
        case 'call':
            return callFunction(node.name, node.args.map(arg => evaluate(arg, ctx)));
        default:
            throw new Error('Unknown AST node');
    }
}

function resolveIdentifier(rowProxy, name) {
    const path = String(name).split('.');
    let current = rowProxy;
    for (const part of path) {
        if (current === null || current === undefined) {
            return undefined;
        }
        if (typeof current === 'object' || typeof current === 'function') {
            current = current[part];
        } else {
            return undefined;
        }
    }
    return current;
}

function truthy(value) {
    return Boolean(value);
}

function areEqual(a, b) {
    if (a instanceof Date && b instanceof Date) {
        return a.getTime() === b.getTime();
    }
    return a === b;
}

function compareScalar(a, b) {
    if (a instanceof Date && b instanceof Date) {
        return a.getTime() - b.getTime();
    }
    if (typeof a === 'number' && typeof b === 'number') {
        return a - b;
    }
    return String(a).localeCompare(String(b));
}

function applyBinary(op, a, b) {
    switch (op) {
        case '+':
            if (typeof a === 'number' && typeof b === 'number') return a + b;
            return String(a ?? '') + String(b ?? '');
        case '-':
            return Number(a) - Number(b);
        case '*':
            return Number(a) * Number(b);
        case '/':
            return Number(a) / Number(b);
        case '%':
            return Number(a) % Number(b);
        case '=':
        case '==':
        case '===':
            return areEqual(a, b);
        case '!=':
        case '!==':
        case '<>':
            return !areEqual(a, b);
        case '>':
            return compareScalar(a, b) > 0;
        case '>=':
            return compareScalar(a, b) >= 0;
        case '<':
            return compareScalar(a, b) < 0;
        case '<=':
            return compareScalar(a, b) <= 0;
        default:
            throw new Error(`Unsupported operator '${op}'`);
    }
}

function callFunction(name, args) {
    const fn = String(name).toUpperCase();
    switch (fn) {
        case 'LEN':
            return String(args[0] ?? '').length;
        case 'UPPER':
            return String(args[0] ?? '').toUpperCase();
        case 'LOWER':
            return String(args[0] ?? '').toLowerCase();
        case 'COALESCE':
            for (const value of args) {
                if (value !== null && value !== undefined) return value;
            }
            return null;
        case 'ISNULL':
            return args[0] === null || args[0] === undefined ? args[1] : args[0];
        case 'IIF':
            return truthy(args[0]) ? args[1] : args[2];
        case 'ABS':
            return Math.abs(Number(args[0]));
        case 'ROUND':
            return typeof args[1] === 'number'
                ? Number(Number(args[0]).toFixed(args[1]))
                : Math.round(Number(args[0]));
        default:
            throw new Error(`Unknown function '${name}'`);
    }
}

module.exports = {
    compileExpression,
    compilePredicate
};

