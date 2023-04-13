module.exports = {
    preset: 'ts-jest',
    moduleFileExtensions: ['js', 'ts'],
    moduleDirectories: ['node_modules', 'src'],
    modulePathIgnorePatterns: ["dist/"],
    transform: {
        '^.+\\.(ts|tsx)?$': 'ts-jest',
    },
    collectCoverage: true,
    collectCoverageFrom: [
        'src/**/*.ts',
    ],
    coveragePathIgnorePatterns: [
        'node_modules',
        'test-config',
        'interfaces',
        'jestGlobalMocks.ts',
        '.mock.ts',
        'sample.ts',
        'dist'
    ],
    coverageReporters: [
        "json-summary", 
        "text",
        "lcov"
    ]
}