from pprint import pprint
from sqlalchemy import func, desc, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    result = session.query(Student.fullname,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade)\
        .join(Student)\
        .group_by(Student.fullname)\
        .order_by(desc('avg_grade'))\
        .limit(5).all()
    return result


def select_2(discipline_id: int):
    result = session.query(Discipline.name,
                           Student.fullname,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .filter(Discipline.id == discipline_id)\
        .group_by(Student.fullname, Discipline.name)\
        .order_by(desc('avg_grade'))\
        .limit(1).all()
    return result


def select_3(discipline_id: int):
    result = session.query(Discipline.name,
                           Group.name,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline) \
        .join(Group) \
        .filter(Discipline.id == discipline_id)\
        .group_by(Group.name, Discipline.name)\
        .order_by(desc('avg_grade')).all()
    return result


def select_4():
    result = session.query(
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).all()
    return result


def select_5(teacher_id: int):
    result = session.query(Discipline.name,
                           Teacher.fullname) \
        .select_from(Discipline)\
        .join(Teacher)\
        .filter(Teacher.id == teacher_id).all()
    return result


def select_6(group_id: int):
    result = session.query(Group.name,
                           Student.fullname) \
        .select_from(Student)\
        .join(Group)\
        .filter(Group.id == group_id).all()
    return result


def select_7(group_id: int, discipline_id: int):
    result = session.query(Group.name,
                           Student.fullname,
                           Grade.grade,
                           Discipline.name) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Group.id == group_id, Discipline.id == discipline_id)).all()
    return result


def select_8(teacher_id: int):
    result = session.query(Discipline.name,
                           Teacher.fullname,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Discipline)\
        .join(Teacher)\
        .join(Grade) \
        .filter(Teacher.id == teacher_id)\
        .group_by(Discipline.name, Teacher.fullname).all()
    return result


def select_9(student_id: int):
    result = session.query(Student.fullname,
                           Discipline.name) \
        .select_from(Grade)\
        .join(Discipline)\
        .join(Student) \
        .filter(Student.id == student_id)\
        .group_by(Discipline.name, Student.fullname).all()
    return result


def select_10(student_id: int, teacher_id: int):
    result = session.query(Student.fullname,
                           Discipline.name,
                           Teacher.fullname) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Student) \
        .join(Teacher)\
        .filter(and_(Student.id == student_id, Teacher.id == teacher_id))\
        .group_by(Discipline.name, Student.fullname, Teacher.fullname).all()
    return result


if __name__ == '__main__':
    pprint(select_10(1, 3))
