# Fundamentals of Data Engineering – Exercise 2 Correction
### Student: Javier Gallardo 

---

## Correction Notes

### Issues to solve:

#### (scrapper) Modify get_songs to use catalog (2 points)
- [ ] Points awarded: 2
- [ ] Comments:

#### (scrapper) Check logs for strange messages (0.5 points)
- [ ] Points awarded: 0
- [ ] Comments:

#### (cleaner) Avoid processing catalogs (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### (Validator) Fix directory creation issue (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### (Validator) Additional validation rule (0.5 points)
- [ ] Points awarded: 0.4
- [ ] Comments: You could have used the 'utils/chords.py' file in the cleaner to not hardcode the chords in the regex. But the rule is good.

#### Code improvements (0.5 points)
- [ ] Points awarded: 0
- [ ] Comments:

### Functionalities to add:

#### 'results' module (0.5 points)
- [ ] Points awarded: 0.3
- [ ] Comments: A single python file is not a module. You should make a directory and put it inside, so you can add libraries and utility functions to it for the main file to call. Also, you need to add ogging features on every process.

#### 'lyrics' module (2 points)
- [ ] Points awarded: 0.4
- [ ] Comments: 
- - A single python file is not a module. You should make a directory and put it inside, so you can add libraries and utility functions to it for the main file to call.
- - It does not work well as it is loosing almost all formatting, deleting almost entire songs in some cases. You need to check the regex pattern.
- - You are not logging anything from this module.
- - You should have considered using the variables in tab_cleaner/utils/chords.py.
- - You are storing the results in the same validation folder. *This is a critical failure*. You should have created a new directory called 'lyrics' inside the files directory so you can access the lyrics only easily.


#### 'insights' module (2 points)
- [ ] Points awarded: 1
- [ ] Comments: Overall I think it is OK, but:
- - A single python file is not a module. You should make a directory and put it inside, so you can add libraries and utility functions to it for the main file to call.
- - You are not logging anything from this module.
- - The STOPWORDS list could have had more words, to be sure you only keep nouns, verbs and adjectives.

#### Main execution file (1 point)
- [ ] Points awarded: 0
- [ ] Comments: Not implemented

---

## Total Score: 5.1 / 10 points

## General Comments:

Your submission demonstrates understanding of the basic concepts, but suffers from incomplete implementation and several technical issues that prevent the pipeline from working properly.
Throughout your submission, you provided single Python files instead of proper modules with directories, utilities, and dependencies. This violates the project architecture established in other modules. The insights module shows good analytical thinking, and your validation rule implementation was solid. You have the technical foundation but need to focus on code quality, testing, and project completion.

