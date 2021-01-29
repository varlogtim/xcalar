testCaseConfig = {
    'retinas': [{
        'name': 'MatrixGenMultiplyLrq',
        'Enabled': True,
        'path': "BUILD_DIR/src/data/qa/MatrixGenMultiply.tar.gz",
        'dstTable': "EndResult-Matrix",
        'retinaParams': {
            'pathToMatrixDatasets':
                '/netstore/datasets/matrixMultiply/partdata/',
        },
    }]
}
