$(document).ready(function() {
    fetchLanguages();

    function fetchLanguages() {
        $.get('/languages', function(data) {
            let languages = '<ul class="list-unstyled">';
            data.forEach(language => {
                languages += `<li><a href="#" onclick="fetchGenres('${language}')">${language}</a></li>`;
            });
            languages += '</ul>';
            $('#language-list').html(languages);
        });
    }

    window.fetchGenres = function(language) {
        $.get(`/genres/${language}`, function(data) {
            let genres = '<ul class="list-unstyled">';
            data.forEach(genre => {
                genres += `<li><a href="#" onclick="fetchMovies('${language}', '${genre}')">${genre}</a></li>`;
            });
            genres += '</ul>';
            $('#language-list').append(genres);
        });
    }

    window.fetchMovies = function(language, genre) {
        $.get(`/movies/${language}/${genre}`, function(data) {
            let movies = '';
            data.forEach(movie => {
                movies += `<tr>
                    <td>${movie.rating}</td>
                    <td>${movie.title}</td>
                    <td>${movie.directors}</td>
                    <td>${movie.cast}</td>
                    <td>${movie.release_date}</td>
                </tr>`;
            });
            $('#movie-table').html(movies);
        });
    }

    window.sortTable = function(sortBy) {
        let table = $('#movie-table');
        let rows = table.find('tr').toArray().sort(compareCells(sortBy));
        table.append(rows);
    }

    function compareCells(sortBy) {
        return function(a, b) {
            let valA = getCellValue(a, sortBy);
            let valB = getCellValue(b, sortBy);
            return $.isNumeric(valA) && $.isNumeric(valB) ? valA - valB : valA.localeCompare(valB);
        };
    }

    function getCellValue(row, sortBy) {
        return $(row).children('td').eq(sortBy === 'rating' ? 0 : 1).text();
    }
});
