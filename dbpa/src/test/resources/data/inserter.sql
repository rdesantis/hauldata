INSERT INTO test.importtarget (number, word)
VALUES
(12345, 'from script'),
(456, 'also from script'),
(9999, 'red');

INSERT INTO test.importtarget (number, word)
VALUES
(99999, 'redundant');

UPDATE test.importtarget
SET word = 'redu redu'
WHERE word = 'redundant';
