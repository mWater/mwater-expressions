gulp = require 'gulp'
coffee = require 'gulp-coffee' 

# Compile coffeescript to js in lib/
gulp.task 'coffee', ->
  gulp.src('./src/**/*.coffee')
    .pipe(coffee({ bare: true }))
    .pipe(gulp.dest('./lib/'))

# Copy non-coffeescript files
gulp.task 'copy', ->
  gulp.src(['./src/**/*.js', './src/**/*.css', './src/**/*.txt'])
    .pipe(gulp.dest('./lib/'))

gulp.task "default", gulp.series("copy", "coffee")
